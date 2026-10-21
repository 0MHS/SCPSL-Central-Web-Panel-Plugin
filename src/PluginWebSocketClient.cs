using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using LabApi.Features.Console;
using LabApi.Features.Wrappers;

namespace ScpWebPanelPlugin
{
    public class PluginWebSocketClient
    {
        private ClientWebSocket _ws;
        private CancellationTokenSource _cts;
        private readonly Config _config;
        private readonly SynchronizationContext _syncContext;
        private bool _isRunning;
        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);

        // Dual queues: prioritized RPC responses, reports, round events vs batched console logs
        private const int MaxPriorityQueueCapacity = 200;
        private const int MaxLogQueueCapacity = 100;
        private readonly ConcurrentQueue<string> _priorityQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _logQueue = new ConcurrentQueue<string>();
        private readonly SemaphoreSlim _queueSignal = new SemaphoreSlim(0);
        private int _priorityCount = 0;
        private int _logCount = 0;
        private Task _senderWorkerTask;
        private Task _batchLogsWorkerTask;
        private readonly ConcurrentQueue<string> _pendingLogs = new ConcurrentQueue<string>();
        private int _pendingLogCount = 0;
        private static readonly object _fileLock = new object();

        public bool IsConnected => _ws != null && _ws.State == WebSocketState.Open;
        public volatile bool IsConsoleSubscribed = false;

        public PluginWebSocketClient(Config config, SynchronizationContext syncContext)
        {
            _config = config;
            _syncContext = syncContext;
        }

        public void Start()
        {
            if (_isRunning) return;
            _isRunning = true;
            _cts = new CancellationTokenSource();
            _senderWorkerTask = Task.Run(() => OutboundSenderWorkerAsync(_cts.Token));
            _batchLogsWorkerTask = Task.Run(() => ConsoleLogBatchWorkerAsync(_cts.Token));
            Task.Run(() => ConnectionLoopAsync(_cts.Token));
        }

        public void Stop()
        {
            _isRunning = false;
            try
            {
                _cts?.Cancel();
                try { _queueSignal.Release(); } catch { }

                if (_ws != null && _ws.State == WebSocketState.Open)
                {
                    _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Plugin stopping", CancellationToken.None).Wait(2000);
                }
                _ws?.Dispose();
                _ws = null;

                while (_priorityQueue.TryDequeue(out _)) { }
                while (_logQueue.TryDequeue(out _)) { }
                while (_pendingLogs.TryDequeue(out _)) { }
                _priorityCount = 0;
                _logCount = 0;
                _pendingLogCount = 0;
            }
            catch { }
        }

        private async Task ConnectionLoopAsync(CancellationToken token)
        {
            int currentDelay = Math.Max(3, _config.ReconnectIntervalSeconds);
            int maxDelay = Math.Max(currentDelay, _config.MaxReconnectIntervalSeconds);

            while (_isRunning && !token.IsCancellationRequested)
            {
                try
                {
                    string wsUrl = _config.CentralWsUrl;
                    if (string.IsNullOrWhiteSpace(wsUrl))
                    {
                        Logger.Warn("[PluginWS] CentralWsUrl is empty. Waiting 10s...");
                        await Task.Delay(10000, token);
                        continue;
                    }

                    Logger.Info($"[PluginWS] Connecting to Central Web Panel at: {wsUrl} ...");

                    _ws?.Dispose();
                    _ws = new ClientWebSocket();

                    // Optional keep-alive interval
                    _ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);

                    using (var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                    using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, connectCts.Token))
                    {
                        await _ws.ConnectAsync(new Uri(wsUrl), linkedCts.Token);
                    }

                    if (_ws.State == WebSocketState.Open)
                    {
                        Logger.Info("[PluginWS] Connected successfully! Sending authentication handshake...");
                        currentDelay = Math.Max(3, _config.ReconnectIntervalSeconds); // Reset backoff

                        // 1. Send Handshake
                        await SendHandshakeAsync(token);

                        // 2. Receive Messages Loop
                        await ReceiveLoopAsync(token);
                    }
                }
                catch (OperationCanceledException)
                {
                    if (token.IsCancellationRequested) break;
                }
                catch (Exception ex)
                {
                    Logger.Warn($"[PluginWS] Connection error: {ex.Message}. Reconnecting in {currentDelay}s...");
                }

                if (!_isRunning || token.IsCancellationRequested) break;

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(currentDelay), token);
                    // Exponential backoff
                    currentDelay = Math.Min(currentDelay * 2, maxDelay);
                }
                catch (OperationCanceledException) { break; }
            }
        }

        private async Task SendHandshakeAsync(CancellationToken token)
        {
            string serverName = EscapeJson(string.IsNullOrWhiteSpace(_config.ServerName) ? $"Server #{Server.Port}" : _config.ServerName);
            int port = Server.Port;

            string authPayload = "{" +
                "\"type\":\"auth\"," +
                $"\"token\":\"{EscapeJson(_config.ServerToken)}\"," +
                $"\"serverName\":\"{serverName}\"," +
                $"\"port\":{port}," +
                $"\"version\":\"2.2.0\"" +
            "}";

            await SendRawAsync(authPayload, token);
        }

        private async Task ReceiveLoopAsync(CancellationToken token)
        {
            byte[] buffer = new byte[16384];

            while (_ws != null && _ws.State == WebSocketState.Open && !token.IsCancellationRequested)
            {
                WebSocketReceiveResult result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Logger.Info("[PluginWS] Central server requested connection close.");
                    await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", token);
                    return;
                }

                string message;
                if (result.EndOfMessage)
                {
                    // Zero-copy direct decode for single-frame messages (virtually all RPC calls and pings)
                    message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                }
                else
                {
                    // Multi-frame fallback for large payloads (e.g. plugin DLL uploads)
                    using (var ms = new MemoryStream())
                    {
                        ms.Write(buffer, 0, result.Count);
                        do
                        {
                            result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                            if (result.MessageType == WebSocketMessageType.Close) return;
                            ms.Write(buffer, 0, result.Count);
                        }
                        while (!result.EndOfMessage);

                        message = Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
                    }
                }

                _ = ProcessIncomingMessageAsync(message);
            }
        }

        private async Task ProcessIncomingMessageAsync(string message)
        {
            if (string.IsNullOrWhiteSpace(message)) return;

            try
            {
                string msgType = ExtractField(message, "type");
                string reqId = ExtractField(message, "id");

                if (msgType == "ping")
                {
                    EnqueuePriority("{\"type\":\"pong\"}");
                    return;
                }

                if (msgType == "stream_control")
                {
                    string stream = ExtractField(message, "stream");
                    string enabledStr = ExtractField(message, "enabled");
                    bool enabled = enabledStr.Equals("true", StringComparison.OrdinalIgnoreCase);
                    if (stream == "console")
                    {
                        IsConsoleSubscribed = enabled;
                        if (!enabled)
                        {
                            while (_pendingLogs.TryDequeue(out _)) { }
                            _pendingLogCount = 0;
                        }
                        Logger.Info($"[PluginWS] Console streaming subscription updated: {enabled}");
                    }
                    return;
                }

                if (msgType == "request")
                {
                    string action = ExtractField(message, "action");
                    await HandleRpcRequestAsync(reqId, action, message);
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Error processing incoming payload: {ex.Message}");
            }
        }

        private async Task HandleRpcRequestAsync(string reqId, string action, string rawMessage)
        {
            try
            {
                switch (action?.ToLower())
                {
                    case "get_players":
                    {
                        var snapshots = await RunOnMainThreadAsync(() =>
                        {
                            try
                            {
                                var list = Player.List;
                                if (list == null) return Array.Empty<PlayerSnapshot>();
                                int count = list.Count;
                                var array = new PlayerSnapshot[count];
                                int idx = 0;
                                foreach (var p in list)
                                {
                                    if (p == null) continue;
                                    if (idx >= array.Length) Array.Resize(ref array, Math.Max(4, array.Length * 2));
                                    array[idx++] = new PlayerSnapshot
                                    {
                                        Id = p.UserId ?? "",
                                        Name = p.Nickname ?? "",
                                        Role = p.Role.ToString() ?? "",
                                        Hp = p.Health,
                                        IsMuted = p.IsMuted
                                    };
                                }
                                if (idx != array.Length) Array.Resize(ref array, idx);
                                return array;
                            }
                            catch
                            {
                                return Array.Empty<PlayerSnapshot>();
                            }
                        });

                        string playersJson = await Task.Run(() => SerializePlayers(snapshots));
                        await SendResponseAsync(reqId, true, playersJson, isRawJson: true);
                        break;
                    }

                    case "kick":
                    {
                        string targetId = SanitizeTargetId(ExtractField(rawMessage, "targetId"));
                        string reason = SanitizeConsoleArgument(ExtractField(rawMessage, "reason"));
                        if (string.IsNullOrEmpty(reason)) reason = "Kicked by Web Panel";

                        if (string.IsNullOrEmpty(targetId))
                        {
                            await SendResponseAsync(reqId, false, "\"Target ID is required\"");
                            return;
                        }

                        bool ok = await RunOnMainThreadAsync(() =>
                        {
                            foreach (var p in Player.List)
                            {
                                if (p != null && (p.UserId == targetId || p.UserId.StartsWith(targetId)))
                                {
                                    p.Kick(reason);
                                    return true;
                                }
                            }
                            return false;
                        });

                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Player not found online\"}", isRawJson: true);
                        break;
                    }

                    case "ban":
                    {
                        string targetId = SanitizeTargetId(ExtractField(rawMessage, "targetId"));
                        string reason = SanitizeConsoleArgument(ExtractField(rawMessage, "reason"));
                        if (string.IsNullOrEmpty(reason)) reason = "Banned by Web Panel";

                        string durationStr = ExtractField(rawMessage, "duration");
                        long durationMinutes = 10;
                        if (long.TryParse(durationStr, out long d)) durationMinutes = d;

                        if (string.IsNullOrEmpty(targetId))
                        {
                            await SendResponseAsync(reqId, false, "\"Target ID is required\"");
                            return;
                        }

                        // Ensure @steam suffix for Steam IDs if missing
                        string formattedTargetId = targetId;
                        if (!formattedTargetId.Contains("@") && formattedTargetId.All(char.IsDigit) && formattedTargetId.Length >= 17)
                        {
                            formattedTargetId += "@steam";
                        }

                        bool isPermanent = durationMinutes <= 0;
                        // For permanent bans, 100 years = 52,560,000 minutes = 3,153,600,000 seconds
                        long finalDurationSec = isPermanent ? 3153600000L : durationMinutes * 60L;

                        string playerName = formattedTargetId;
                        bool ok = await RunOnMainThreadAsync(() =>
                        {
                            try
                            {
                                // 1. Main-thread: Find online player, capture name and execute instant kick
                                foreach (var p in Player.List)
                                {
                                    if (p != null && (p.UserId == formattedTargetId || p.UserId == targetId || p.UserId.StartsWith(targetId)))
                                    {
                                        if (!string.IsNullOrEmpty(p.Nickname)) playerName = SanitizeConsoleArgument(p.Nickname, 64);
                                        try { p.Kick(reason); } catch { }
                                        break;
                                    }
                                }

                                // 2. Main-thread: Dispatch ban console command
                                try
                                {
                                    ServerConsole.EnterCommand($"ban {formattedTargetId} {finalDurationSec} {reason}");
                                }
                                catch (Exception bEx)
                                {
                                    Logger.Warn($"[PluginWS] ServerConsole ban command failed: {bEx.Message}");
                                }

                                return true;
                            }
                            catch (Exception ex)
                            {
                                Logger.Error($"[PluginWS] Error executing ban on main thread: {ex.Message}");
                                return false;
                            }
                        });

                        // 3. Background Task: Offload fallback UserIdBans.txt disk file I/O off the main thread to protect tickrate
                        string capturedPlayerName = playerName;
                        _ = Task.Run(() =>
                        {
                            try
                            {
                                string bansPath = ResolveConfigFilePath("UserIdBans.txt");
                                if (!string.IsNullOrEmpty(bansPath))
                                {
                                    lock (_fileLock)
                                    {
                                        string existing = File.Exists(bansPath) ? File.ReadAllText(bansPath) : "";
                                        if (!existing.Contains(formattedTargetId) && !existing.Contains(targetId))
                                        {
                                            DateTime nowUtc = DateTime.UtcNow;
                                            DateTime expUtc = isPermanent ? nowUtc.AddYears(100) : nowUtc.AddMinutes(durationMinutes);
                                            long expTicks = expUtc.Ticks;
                                            long issTicks = nowUtc.Ticks;

                                            string banLine = $"{capturedPlayerName};{formattedTargetId};{expTicks};{reason};Web Panel;{issTicks}";
                                            string newContent = string.IsNullOrEmpty(existing.Trim()) ? banLine : existing.TrimEnd() + Environment.NewLine + banLine + Environment.NewLine;
                                            File.WriteAllText(bansPath, newContent);
                                            _ = RunOnMainThreadAsync(() =>
                                            {
                                                try { ServerConsole.EnterCommand("reload config"); } catch { }
                                                return true;
                                            });
                                        }
                                    }
                                }
                            }
                            catch (Exception fEx)
                            {
                                Logger.Warn($"[PluginWS] Direct UserIdBans.txt write fallback failed: {fEx.Message}");
                            }
                        });

                        await SendResponseAsync(reqId, ok, "{\"success\":true}", isRawJson: true);
                        break;
                    }

                    case "mute":
                    {
                        string targetId = SanitizeTargetId(ExtractField(rawMessage, "targetId"));
                        string reason = SanitizeConsoleArgument(ExtractField(rawMessage, "reason"));
                        if (string.IsNullOrEmpty(reason)) reason = "Muted by Web Panel";

                        string durationStr = ExtractField(rawMessage, "duration");
                        long durationMinutes = 10;
                        if (long.TryParse(durationStr, out long d)) durationMinutes = d;

                        if (string.IsNullOrEmpty(targetId))
                        {
                            await SendResponseAsync(reqId, false, "\"Target ID is required\"");
                            return;
                        }

                        bool ok = await RunOnMainThreadAsync(() =>
                        {
                            foreach (var p in Player.List)
                            {
                                if (p != null && (p.UserId == targetId || p.UserId.StartsWith(targetId)))
                                {
                                    VoiceChat.VoiceChatMutes.IssueLocalMute(p.UserId, false);
                                    Logger.Info($"[PluginWS] Muted {p.Nickname} ({p.UserId}) for {durationMinutes}m. Reason: {reason}");
                                    return true;
                                }
                            }
                            return false;
                        });

                        // Dangling unmanaged Task.Delay removed. Active mutes and durations are tracked and revoked centrally by Web Panel.
                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Player not found online\"}", isRawJson: true);
                        break;
                    }

                    case "unmute":
                    {
                        string targetId = SanitizeTargetId(ExtractField(rawMessage, "targetId"));
                        if (string.IsNullOrEmpty(targetId))
                        {
                            await SendResponseAsync(reqId, false, "\"Target ID is required\"");
                            return;
                        }

                        bool ok = await RunOnMainThreadAsync(() =>
                        {
                            foreach (var p in Player.List)
                            {
                                if (p != null && (p.UserId == targetId || p.UserId.StartsWith(targetId)))
                                {
                                    VoiceChat.VoiceChatMutes.RevokeLocalMute(p.UserId, false);
                                    Logger.Info($"[PluginWS] Unmuted {p.Nickname} ({p.UserId})");
                                    return true;
                                }
                            }
                            return false;
                        });

                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Player not found online\"}", isRawJson: true);
                        break;
                    }

                    case "broadcast":
                    {
                        string message = ExtractField(rawMessage, "message");
                        string targetId = SanitizeTargetId(ExtractField(rawMessage, "targetId"));
                        string durationStr = ExtractField(rawMessage, "duration");
                        ushort duration = 10;
                        if (ushort.TryParse(durationStr, out ushort d) && d > 0) duration = d;

                        if (string.IsNullOrWhiteSpace(message))
                        {
                            await SendResponseAsync(reqId, false, "\"Broadcast message is required\"");
                            return;
                        }

                        bool ok = await RunOnMainThreadAsync(() =>
                        {
                            try
                            {
                                if (!string.IsNullOrEmpty(targetId))
                                {
                                    // Player-specific broadcast (pbc)
                                    foreach (var p in Player.List)
                                    {
                                        if (p != null && (p.UserId == targetId || p.UserId.StartsWith(targetId) || p.PlayerId.ToString() == targetId))
                                        {
                                            p.SendBroadcast(message, duration, Broadcast.BroadcastFlags.Normal, true);
                                            Logger.Info($"[PluginWS] Broadcast sent to {p.Nickname} ({p.UserId}): \"{message}\" ({duration}s)");
                                            return true;
                                        }
                                    }
                                    return false;
                                }
                                else
                                {
                                    // Global broadcast (bc)
                                    LabApi.Features.Wrappers.Server.SendBroadcast(message, duration, Broadcast.BroadcastFlags.Normal, true);
                                    Logger.Info($"[PluginWS] Global broadcast dispatched: \"{message}\" ({duration}s)");
                                    return true;
                                }
                            }
                            catch (Exception ex)
                            {
                                Logger.Error($"[PluginWS] Failed to send broadcast: {ex.Message}");
                                return false;
                            }
                        });

                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Target player not found online or broadcast failed\"}", isRawJson: true);
                        break;
                    }

                    case "command":
                    {
                        string cmd = ExtractField(rawMessage, "command");
                        if (string.IsNullOrWhiteSpace(cmd))
                        {
                            await SendResponseAsync(reqId, false, "\"Command is required\"");
                            return;
                        }

                        string commandOutput = await RunOnMainThreadAsync(() =>
                        {
                            try
                            {
                                string trimmed = cmd.Trim();
                                string lower = trimmed.ToLowerInvariant();

                                // Intercept server restart aliases to ensure full process reboot rather than match round restart
                                if (lower == "restart" || lower == "server_restart" || lower == "serverrestart")
                                {
                                    ServerConsole.EnterCommand("srestart");
                                    return "[Server] Soft restart command sent ('srestart'). Server process is restarting...";
                                }

                                // 1. Direct handling for broadcast commands (bc / pbc)
                                if (lower.StartsWith("bc ") || lower == "bc")
                                {
                                    string[] parts = trimmed.Split(new[] { ' ' }, 3, StringSplitOptions.RemoveEmptyEntries);
                                    if (parts.Length >= 2)
                                    {
                                        ushort duration = 10;
                                        string msg;
                                        if (ushort.TryParse(parts[1], out ushort parsedDur) && parts.Length >= 3)
                                        {
                                            duration = parsedDur;
                                            msg = parts[2];
                                        }
                                        else
                                        {
                                            msg = trimmed.Substring(3).Trim();
                                        }

                                        LabApi.Features.Wrappers.Server.SendBroadcast(msg, duration, Broadcast.BroadcastFlags.Normal, true);
                                        return $"Broadcast sent to all players ({duration}s): {msg}";
                                    }
                                }
                                else if (lower.StartsWith("pbc ") || lower == "pbc")
                                {
                                    string[] parts = trimmed.Split(new[] { ' ' }, 4, StringSplitOptions.RemoveEmptyEntries);
                                    if (parts.Length >= 3)
                                    {
                                        string target = parts[1];
                                        ushort duration = 10;
                                        string msg;
                                        if (ushort.TryParse(parts[2], out ushort parsedDur) && parts.Length >= 4)
                                        {
                                            duration = parsedDur;
                                            msg = parts[3];
                                        }
                                        else
                                        {
                                            msg = trimmed.Substring(parts[0].Length + parts[1].Length + 2).Trim();
                                        }

                                        foreach (var p in Player.List)
                                        {
                                            if (p != null && (p.UserId == target || p.UserId.StartsWith(target) || p.PlayerId.ToString() == target))
                                            {
                                                p.SendBroadcast(msg, duration, Broadcast.BroadcastFlags.Normal, true);
                                                return $"Player broadcast sent to {p.Nickname} ({duration}s): {msg}";
                                            }
                                        }
                                        return $"Player '{target}' not found online.";
                                    }
                                }

                                // 2. Attempt execution via LabApi Server.RunCommand
                                string labResult = null;
                                try
                                {
                                    labResult = LabApi.Features.Wrappers.Server.RunCommand(trimmed, ServerConsole.Scs);
                                }
                                catch { }

                                if (!string.IsNullOrWhiteSpace(labResult) && 
                                    !labResult.Contains("isn't available") && 
                                    !labResult.Contains("Command not found") &&
                                    !labResult.Contains("Command does not exist"))
                                {
                                    return labResult.Trim();
                                }

                                // 3. Fallback: execute via ServerConsole (Game Console commands like roundrestart, etc.)
                                ServerConsole.EnterCommand(trimmed);
                                return string.IsNullOrWhiteSpace(labResult) ? $"[Server] Executed: {trimmed}" : labResult.Trim();
                            }
                            catch (Exception ex)
                            {
                                return $"[Error] {ex.Message}";
                            }
                        });

                        await SendResponseAsync(reqId, true, $"{{\"result\":\"{EscapeJson(commandOutput)}\"}}", isRawJson: true);
                        break;
                    }

                    case "download_plugin":
                    {
                        string name = ExtractField(rawMessage, "name");
                        string pType = ExtractField(rawMessage, "pluginType")?.ToLower();
                        if (string.IsNullOrWhiteSpace(name))
                        {
                            await SendResponseAsync(reqId, false, "\"Plugin name is required\"");
                            return;
                        }

                        string targetFile = null;
                        try
                        {
                            var pluginsDir = LabApi.Loader.Features.Paths.PathManager.Plugins;
                            if (pluginsDir != null && pluginsDir.Exists)
                            {
                                foreach (var f in pluginsDir.GetFiles("*.dll", SearchOption.AllDirectories))
                                {
                                    if (f.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                                        Path.GetFileNameWithoutExtension(f.Name).Equals(name, StringComparison.OrdinalIgnoreCase))
                                    {
                                        targetFile = f.FullName;
                                        break;
                                    }
                                }
                            }

                            if (targetFile == null && LabApi.Loader.Features.Paths.PathManager.Dependencies != null && LabApi.Loader.Features.Paths.PathManager.Dependencies.Exists)
                            {
                                foreach (var f in LabApi.Loader.Features.Paths.PathManager.Dependencies.GetFiles("*.dll", SearchOption.AllDirectories))
                                {
                                    if (f.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                                        Path.GetFileNameWithoutExtension(f.Name).Equals(name, StringComparison.OrdinalIgnoreCase))
                                    {
                                        targetFile = f.FullName;
                                        break;
                                    }
                                }
                            }
                        }
                        catch { }

                        if (targetFile != null && File.Exists(targetFile))
                        {
                            byte[] fileBytes = File.ReadAllBytes(targetFile);
                            string b64 = Convert.ToBase64String(fileBytes);
                            string safeName = Path.GetFileName(targetFile);
                            await SendResponseAsync(reqId, true, $"{{\"name\":\"{EscapeJson(safeName)}\",\"data\":\"{b64}\"}}", isRawJson: true);
                        }
                        else
                        {
                            await SendResponseAsync(reqId, false, "\"Plugin file not found on game server\"");
                        }
                        break;
                    }

                    case "upload_plugin":
                    {
                        string name = ExtractField(rawMessage, "name");
                        string b64 = ExtractField(rawMessage, "data");
                        string isDepStr = ExtractField(rawMessage, "isDependency");
                        bool isDep = isDepStr == "true";

                        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(b64))
                        {
                            await SendResponseAsync(reqId, false, "\"Name and data are required\"");
                            return;
                        }

                        try
                        {
                            byte[] fileBytes = Convert.FromBase64String(b64);
                            string safeFileName = Path.GetFileName(name);
                            if (!safeFileName.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                            {
                                safeFileName += ".dll";
                            }

                            string targetDir = isDep && LabApi.Loader.Features.Paths.PathManager.Dependencies != null
                                ? LabApi.Loader.Features.Paths.PathManager.Dependencies.FullName
                                : (LabApi.Loader.Features.Paths.PathManager.Plugins != null ? LabApi.Loader.Features.Paths.PathManager.Plugins.FullName : "");

                            if (string.IsNullOrEmpty(targetDir))
                            {
                                targetDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "SCP Secret Laboratory", "PluginAPI", "plugins");
                            }

                            // If global directory exists inside plugins/dependencies, prioritize global
                            string globalSubDir = Path.Combine(targetDir, "global");
                            if (Directory.Exists(globalSubDir))
                            {
                                targetDir = globalSubDir;
                            }

                            if (!Directory.Exists(targetDir)) Directory.CreateDirectory(targetDir);
                            string destPath = Path.Combine(targetDir, safeFileName);
                            
                            // Safe atomic file replacement: write to temp file then move/replace
                            string tmpPath = destPath + ".tmp";
                            File.WriteAllBytes(tmpPath, fileBytes);
                            try
                            {
                                if (File.Exists(destPath))
                                {
                                    string bakPath = destPath + ".bak";
                                    try { if (File.Exists(bakPath)) File.Delete(bakPath); } catch { }
                                    try { File.Move(destPath, bakPath); } catch { File.Delete(destPath); }
                                }
                                File.Move(tmpPath, destPath);
                            }
                            catch
                            {
                                // Fallback to direct write if move failed
                                File.WriteAllBytes(destPath, fileBytes);
                                try { if (File.Exists(tmpPath)) File.Delete(tmpPath); } catch { }
                            }
                            Logger.Info($"[PluginWS] Wrote uploaded plugin to {destPath} ({fileBytes.Length} bytes)");

                            // Also sync to global directory if targetDir wasn't global already
                            try
                            {
                                if (!targetDir.EndsWith("global", StringComparison.OrdinalIgnoreCase) && Directory.Exists(globalSubDir))
                                {
                                    string globalDest = Path.Combine(globalSubDir, safeFileName);
                                    string globalTmp = globalDest + ".tmp";
                                    File.WriteAllBytes(globalTmp, fileBytes);
                                    try
                                    {
                                        if (File.Exists(globalDest))
                                        {
                                            string globalBak = globalDest + ".bak";
                                            try { if (File.Exists(globalBak)) File.Delete(globalBak); } catch { }
                                            try { File.Move(globalDest, globalBak); } catch { File.Delete(globalDest); }
                                        }
                                        File.Move(globalTmp, globalDest);
                                    }
                                    catch
                                    {
                                        File.WriteAllBytes(globalDest, fileBytes);
                                        try { if (File.Exists(globalTmp)) File.Delete(globalTmp); } catch { }
                                    }
                                }
                            }
                            catch { }

                            await RunOnMainThreadAsync(() =>
                            {
                                try { ServerConsole.EnterCommand("labapi reload configs"); } catch { }
                                return true;
                            });

                            await SendResponseAsync(reqId, true, $"{{\"success\":true,\"name\":\"{EscapeJson(safeFileName)}\",\"size\":{fileBytes.Length},\"requiresRestart\":true}}", isRawJson: true);
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"[PluginWS] Failed to save uploaded plugin: {ex.Message}");
                            await SendResponseAsync(reqId, false, $"\"{EscapeJson(ex.Message)}\"");
                        }
                        break;
                    }

                    case "delete_plugin":
                    {
                        string name = ExtractField(rawMessage, "name");
                        string pType = ExtractField(rawMessage, "pluginType")?.ToLower();
                        if (string.IsNullOrWhiteSpace(name))
                        {
                            await SendResponseAsync(reqId, false, "\"Plugin name is required\"");
                            return;
                        }

                        bool deleted = false;
                        try
                        {
                            var pluginsDir = LabApi.Loader.Features.Paths.PathManager.Plugins;
                            if (pluginsDir != null && pluginsDir.Exists)
                            {
                                foreach (var f in pluginsDir.GetFiles("*.dll", SearchOption.AllDirectories))
                                {
                                    if (f.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                                        Path.GetFileNameWithoutExtension(f.Name).Equals(name, StringComparison.OrdinalIgnoreCase))
                                    {
                                        f.Delete();
                                        deleted = true;
                                        Logger.Info($"[PluginWS] Deleted plugin file: {f.FullName}");
                                    }
                                }
                            }

                            if (!deleted && LabApi.Loader.Features.Paths.PathManager.Dependencies != null && LabApi.Loader.Features.Paths.PathManager.Dependencies.Exists)
                            {
                                foreach (var f in LabApi.Loader.Features.Paths.PathManager.Dependencies.GetFiles("*.dll", SearchOption.AllDirectories))
                                {
                                    if (f.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                                        Path.GetFileNameWithoutExtension(f.Name).Equals(name, StringComparison.OrdinalIgnoreCase))
                                    {
                                        f.Delete();
                                        deleted = true;
                                        Logger.Info($"[PluginWS] Deleted dependency file: {f.FullName}");
                                    }
                                }
                            }
                        }
                        catch (Exception delEx)
                        {
                            Logger.Error($"[PluginWS] Error deleting plugin '{name}': {delEx.Message}");
                        }

                        await SendResponseAsync(reqId, deleted, deleted ? "{\"success\":true}" : "{\"error\":\"File not found or locked\"}", isRawJson: true);
                        break;
                    }

                    case "get_plugin_configs_list":
                    {
                        var configList = new List<string>();
                        try
                        {
                            var configsDir = LabApi.Loader.Features.Paths.PathManager.Configs;
                            if (configsDir != null && configsDir.Exists)
                            {
                                // Port directory (e.g. configs/7777/)
                                int port = Server.Port;
                                string portDir = Path.Combine(configsDir.FullName, port.ToString());
                                if (Directory.Exists(portDir))
                                {
                                    foreach (var subDir in Directory.GetDirectories(portDir))
                                    {
                                        string pluginName = Path.GetFileName(subDir);
                                        string cfgFile = Path.Combine(subDir, "config.yml");
                                        if (File.Exists(cfgFile))
                                        {
                                            configList.Add($"{{\"name\":\"{EscapeJson(pluginName)}\",\"file\":\"config.yml\"}}");
                                        }
                                    }
                                }

                                // Global configs in configs/
                                foreach (var subDir in Directory.GetDirectories(configsDir.FullName))
                                {
                                    string dirName = Path.GetFileName(subDir);
                                    if (int.TryParse(dirName, out _)) continue; // skip port folders
                                    string cfgFile = Path.Combine(subDir, "config.yml");
                                    if (File.Exists(cfgFile) && !configList.Any(c => c.Contains($"\"{EscapeJson(dirName)}\"")))
                                    {
                                        configList.Add($"{{\"name\":\"{EscapeJson(dirName)}\",\"file\":\"config.yml\"}}");
                                    }
                                }

                                // Check root permissions.yml
                                string permFile = Path.Combine(configsDir.FullName, "permissions.yml");
                                if (File.Exists(permFile))
                                {
                                    configList.Add($"{{\"name\":\"LabAPI Permissions\",\"file\":\"permissions.yml\"}}");
                                }
                            }
                        }
                        catch (Exception cfgEx)
                        {
                            Logger.Error($"[PluginWS] Failed listing plugin configs: {cfgEx.Message}");
                        }

                        string configsJson = "[" + string.Join(",", configList) + "]";
                        await SendResponseAsync(reqId, true, configsJson, isRawJson: true);
                        break;
                    }

                    case "get_plugin_config":
                    {
                        string pluginName = ExtractField(rawMessage, "pluginName");
                        if (string.IsNullOrWhiteSpace(pluginName))
                        {
                            await SendResponseAsync(reqId, false, "\"pluginName is required\"");
                            return;
                        }

                        string resolvedPath = null;
                        try
                        {
                            var configsDir = LabApi.Loader.Features.Paths.PathManager.Configs;
                            if (configsDir != null && configsDir.Exists)
                            {
                                int port = Server.Port;
                                if (pluginName.Equals("LabAPI Permissions", StringComparison.OrdinalIgnoreCase) || pluginName.Equals("permissions", StringComparison.OrdinalIgnoreCase))
                                {
                                    string permFile = Path.Combine(configsDir.FullName, "permissions.yml");
                                    if (File.Exists(permFile)) resolvedPath = permFile;
                                }
                                else
                                {
                                    string portCfg = Path.Combine(configsDir.FullName, port.ToString(), pluginName, "config.yml");
                                    if (File.Exists(portCfg))
                                    {
                                        resolvedPath = portCfg;
                                    }
                                    else
                                    {
                                        string globalCfg = Path.Combine(configsDir.FullName, pluginName, "config.yml");
                                        if (File.Exists(globalCfg)) resolvedPath = globalCfg;
                                    }
                                }
                            }
                        }
                        catch { }

                        if (resolvedPath != null && File.Exists(resolvedPath))
                        {
                            string content = File.ReadAllText(resolvedPath);
                            await SendResponseAsync(reqId, true, $"\"{EscapeJson(content)}\"", isRawJson: true);
                        }
                        else
                        {
                            await SendResponseAsync(reqId, false, "\"Configuration file not found for plugin\"");
                        }
                        break;
                    }

                    case "save_plugin_config":
                    {
                        string pluginName = ExtractField(rawMessage, "pluginName");
                        string content = ExtractField(rawMessage, "content");

                        if (string.IsNullOrWhiteSpace(pluginName))
                        {
                            await SendResponseAsync(reqId, false, "\"pluginName is required\"");
                            return;
                        }

                        string resolvedPath = null;
                        try
                        {
                            var configsDir = LabApi.Loader.Features.Paths.PathManager.Configs;
                            if (configsDir != null && configsDir.Exists)
                            {
                                int port = Server.Port;
                                if (pluginName.Equals("LabAPI Permissions", StringComparison.OrdinalIgnoreCase) || pluginName.Equals("permissions", StringComparison.OrdinalIgnoreCase))
                                {
                                    resolvedPath = Path.Combine(configsDir.FullName, "permissions.yml");
                                }
                                else
                                {
                                    string portDir = Path.Combine(configsDir.FullName, port.ToString(), pluginName);
                                    if (!Directory.Exists(portDir)) Directory.CreateDirectory(portDir);
                                    resolvedPath = Path.Combine(portDir, "config.yml");
                                }
                            }
                        }
                        catch { }

                        if (resolvedPath != null)
                        {
                            File.WriteAllText(resolvedPath, content ?? "");
                            Logger.Info($"[PluginWS] Saved config for plugin '{pluginName}' at {resolvedPath}");

                            await RunOnMainThreadAsync(() =>
                            {
                                try { ServerConsole.EnterCommand("labapi reload configs"); } catch { }
                                return true;
                            });

                            await SendResponseAsync(reqId, true, "{\"success\":true}", isRawJson: true);
                        }
                        else
                        {
                            await SendResponseAsync(reqId, false, "\"Could not resolve config file path\"");
                        }
                        break;
                    }

                    case "get_bans_file":
                    {
                        string content = ReadConfigFile("UserIdBans.txt");
                        await SendResponseAsync(reqId, true, $"\"{EscapeJson(content)}\"", isRawJson: true);
                        break;
                    }

                    case "save_bans_file":
                    {
                        string content = ExtractField(rawMessage, "content");
                        bool ok = WriteConfigFile("UserIdBans.txt", content);
                        if (ok)
                        {
                            await RunOnMainThreadAsync(() =>
                            {
                                ServerConsole.EnterCommand("reload config");
                                return true;
                            });
                        }
                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Failed to write bans file\"}", isRawJson: true);
                        break;
                    }

                    case "get_reserved_slots_file":
                    {
                        string content = ReadConfigFile("UserIDReservedSlots.txt");
                        await SendResponseAsync(reqId, true, $"\"{EscapeJson(content)}\"", isRawJson: true);
                        break;
                    }

                    case "save_reserved_slots_file":
                    {
                        string content = ExtractField(rawMessage, "content");
                        bool ok = WriteConfigFile("UserIDReservedSlots.txt", content);
                        if (ok)
                        {
                            await RunOnMainThreadAsync(() =>
                            {
                                ServerConsole.EnterCommand("reload config");
                                return true;
                            });
                        }
                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Failed to write reserved slots file\"}", isRawJson: true);
                        break;
                    }

                    case "get_ra_config":
                    {
                        string content = ReadConfigFile("config_remoteadmin.txt");
                        await SendResponseAsync(reqId, true, $"\"{EscapeJson(content)}\"", isRawJson: true);
                        break;
                    }

                    case "save_ra_config":
                    {
                        string content = ExtractField(rawMessage, "content");
                        bool ok = WriteConfigFile("config_remoteadmin.txt", content);
                        if (ok)
                        {
                            await RunOnMainThreadAsync(() =>
                            {
                                ServerConsole.EnterCommand("reload config");
                                return true;
                            });
                        }
                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Failed to write RA config file\"}", isRawJson: true);
                        break;
                    }

                    case "get_gameplay_config":
                    {
                        string content = ReadConfigFile("config_gameplay.txt");
                        await SendResponseAsync(reqId, true, $"\"{EscapeJson(content)}\"", isRawJson: true);
                        break;
                    }

                    case "save_gameplay_config":
                    {
                        string content = ExtractField(rawMessage, "content");
                        bool ok = WriteConfigFile("config_gameplay.txt", content);
                        if (ok)
                        {
                            await RunOnMainThreadAsync(() =>
                            {
                                ServerConsole.EnterCommand("reload config");
                                return true;
                            });
                        }
                        await SendResponseAsync(reqId, ok, ok ? "{\"success\":true}" : "{\"error\":\"Failed to write gameplay config file\"}", isRawJson: true);
                        break;
                    }

                    case "get_plugins":
                    {
                        string pluginsJson = GetPluginsJson();
                        await SendResponseAsync(reqId, true, pluginsJson, isRawJson: true);
                        break;
                    }

                    case "get_status":
                    {
                        var mainSnap = await RunOnMainThreadAsync(() =>
                        {
                            int pCount = 0;
                            try { pCount = Player.List != null ? Player.List.Count : 0; } catch { }
                            return new StatusSnapshot
                            {
                                Tps = Server.Tps,
                                PlayerCount = pCount,
                                MaxPlayers = Server.MaxPlayers,
                                Port = Server.Port
                            };
                        });

                        string statusJson = await Task.Run(() => BuildServerStatusJson(mainSnap));
                        await SendResponseAsync(reqId, true, statusJson, isRawJson: true);
                        break;
                    }

                    default:
                    {
                        await SendResponseAsync(reqId, false, $"\"Unknown action: {EscapeJson(action)}\"");
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Error handling RPC '{action}': {ex.Message}");
                await SendResponseAsync(reqId, false, $"\"{EscapeJson(ex.Message)}\"");
            }
        }

        private void EnqueuePriority(string json)
        {
            if (!_isRunning || !_config.IsEnabled) return;

            if (Interlocked.Increment(ref _priorityCount) > MaxPriorityQueueCapacity)
            {
                if (_priorityQueue.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _priorityCount);
                    _priorityQueue.Enqueue(json);
                    return; // Replaced an existing item; permit already exists in _queueSignal
                }
                Interlocked.Decrement(ref _priorityCount);
                return;
            }

            _priorityQueue.Enqueue(json);
            _queueSignal.Release();
        }

        private void EnqueueLog(string json)
        {
            if (!_isRunning || !_config.IsEnabled) return;

            if (Interlocked.Increment(ref _logCount) > MaxLogQueueCapacity)
            {
                if (_logQueue.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _logCount);
                    _logQueue.Enqueue(json);
                    return; // Replaced an existing item; permit already exists in _queueSignal
                }
                Interlocked.Decrement(ref _logCount);
                return;
            }

            _logQueue.Enqueue(json);
            _queueSignal.Release();
        }

        private async Task OutboundSenderWorkerAsync(CancellationToken token)
        {
            while (_isRunning && !token.IsCancellationRequested)
            {
                try
                {
                    await _queueSignal.WaitAsync(token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                string message = null;
                // Dual queue scheduling: strictly prioritize RPC responses, reports, and round stats over console logs
                if (_priorityQueue.TryDequeue(out message))
                {
                    Interlocked.Decrement(ref _priorityCount);
                }
                else if (_logQueue.TryDequeue(out message))
                {
                    Interlocked.Decrement(ref _logCount);
                }

                if (message != null && IsConnected)
                {
                    await SendRawAsync(message, token);
                }
            }
        }

        private async Task ConsoleLogBatchWorkerAsync(CancellationToken token)
        {
            while (_isRunning && !token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(100, token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (!_isRunning || !IsConnected || !IsConsoleSubscribed || _pendingLogs.IsEmpty) continue;

                var batch = new List<string>(20);
                while (batch.Count < 20 && _pendingLogs.TryDequeue(out string line))
                {
                    Interlocked.Decrement(ref _pendingLogCount);
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        batch.Add(line);
                    }
                }

                if (batch.Count == 0) continue;

                // Flush up to 20 lines every 100ms in a single TCP frame
                var sb = new StringBuilder(batch.Count * 96 + 64);
                sb.Append("{\"type\":\"console_logs\",\"messages\":[");
                for (int i = 0; i < batch.Count; i++)
                {
                    if (i > 0) sb.Append(',');
                    sb.Append('"');
                    sb.Append(EscapeJson(batch[i]));
                    sb.Append('"');
                }
                sb.Append("],\"timestamp\":\"");
                sb.Append(DateTime.UtcNow.ToString("O"));
                sb.Append("\"}");

                EnqueueLog(sb.ToString());
            }
        }

        private Task SendResponseAsync(string reqId, bool success, string dataPayload, bool isRawJson = false)
        {
            string json = "{" +
                "\"type\":\"response\"," +
                $"\"id\":\"{EscapeJson(reqId)}\"," +
                $"\"success\":{(success ? "true" : "false")}," +
                $"\"data\":{(isRawJson ? dataPayload : $"\"{EscapeJson(dataPayload)}\"")}" +
            "}";

            // Responses to RPC commands are strictly prioritized ahead of console logs in the outbound queue
            EnqueuePriority(json);
            return Task.CompletedTask;
        }

        public Task SendEventAsync(string eventName, string jsonPayload)
        {
            if (!IsConnected) return Task.CompletedTask;

            string envelope = "{" +
                "\"type\":\"event\"," +
                $"\"event\":\"{EscapeJson(eventName)}\"," +
                $"\"payload\":{jsonPayload}" +
            "}";

            EnqueuePriority(envelope);
            return Task.CompletedTask;
        }

        public Task SendConsoleLogAsync(string logLine)
        {
            if (!IsConnected || !_config.StreamConsoleLogs || !IsConsoleSubscribed) return Task.CompletedTask;
            if (string.IsNullOrWhiteSpace(logLine)) return Task.CompletedTask;

            // Bounded pending queue with zero-lock atomic counter to avoid memory ballooning if network stalls
            if (Interlocked.Increment(ref _pendingLogCount) <= 300)
            {
                _pendingLogs.Enqueue(logLine);
            }
            else
            {
                Interlocked.Decrement(ref _pendingLogCount);
            }
            return Task.CompletedTask;
        }

        private async Task SendRawAsync(string json, CancellationToken token)
        {
            if (_ws == null || _ws.State != WebSocketState.Open) return;

            byte[] bytes = Encoding.UTF8.GetBytes(json);
            try
            {
                using (var sendTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
                using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, sendTimeoutCts.Token))
                {
                    await _sendLock.WaitAsync(linkedCts.Token);
                    try
                    {
                        if (_ws != null && _ws.State == WebSocketState.Open)
                        {
                            await _ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, linkedCts.Token);
                        }
                    }
                    finally
                    {
                        _sendLock.Release();
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Warn($"[PluginWS] Failed to send WS message: {ex.Message}");
            }
        }

        private string ReadConfigFile(string fileName)
        {
            try
            {
                string path = ResolveConfigFilePath(fileName);
                if (File.Exists(path))
                {
                    return File.ReadAllText(path);
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Failed to read {fileName}: {ex.Message}");
            }
            return "";
        }

        private bool WriteConfigFile(string fileName, string content)
        {
            try
            {
                string path = ResolveConfigFilePath(fileName);
                string dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);
                string cleanContent = content ?? "";
                if (cleanContent.Contains("\\n") && !cleanContent.Contains("\n"))
                {
                    cleanContent = cleanContent.Replace("\\r\\n", "\n").Replace("\\n", "\n").Replace("\\r", "\n");
                }
                cleanContent = cleanContent.Replace("\r\n", "\n").Replace("\r", "\n");
                lock (_fileLock)
                {
                    File.WriteAllText(path, cleanContent.Replace("\n", Environment.NewLine));
                }
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Failed to write {fileName}: {ex.Message}");
                return false;
            }
        }

        private static readonly ConcurrentDictionary<string, string> _configPathCache = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        private string ResolveConfigFilePath(string fileName)
        {
            if (string.IsNullOrEmpty(fileName)) return fileName;

            if (_configPathCache.TryGetValue(fileName, out string cachedPath) && File.Exists(cachedPath))
            {
                return cachedPath;
            }

            string resolved = ResolveConfigFilePathInternal(fileName);
            if (!string.IsNullOrEmpty(resolved) && File.Exists(resolved))
            {
                _configPathCache[fileName] = resolved;
            }
            return resolved;
        }

        private string ResolveConfigFilePathInternal(string fileName)
        {
            try
            {
                string lower = fileName.ToLowerInvariant();

                // 1. Check LabApi PathManager properties directly
                try
                {
                    if (lower.Contains("remoteadmin"))
                    {
                        string p = LabApi.Loader.Features.Paths.PathManager.RAConfigPath;
                        if (!string.IsNullOrEmpty(p) && File.Exists(p)) return p;
                    }
                    else if (lower.Contains("gameplay"))
                    {
                        string p = LabApi.Loader.Features.Paths.PathManager.GameplayConfigPath;
                        if (!string.IsNullOrEmpty(p) && File.Exists(p)) return p;
                    }
                    else if (lower.Contains("bans"))
                    {
                        string p = LabApi.Loader.Features.Paths.PathManager.UserIdBansPath;
                        if (!string.IsNullOrEmpty(p) && File.Exists(p)) return p;
                    }
                    else if (lower.Contains("reserved"))
                    {
                        string p = LabApi.Loader.Features.Paths.PathManager.ReservedSlotsConfigPath;
                        if (!string.IsNullOrEmpty(p) && File.Exists(p)) return p;
                    }

                    if (LabApi.Loader.Features.Paths.PathManager.Configs != null)
                    {
                        string candidate = Path.Combine(LabApi.Loader.Features.Paths.PathManager.Configs.FullName, fileName);
                        if (File.Exists(candidate)) return candidate;
                    }
                }
                catch { }

                // 2. Port config directory in AppData (Windows)
                string appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
                string portDir = Path.Combine(appData, "SCP Secret Laboratory", "config", Server.Port.ToString(), fileName);
                if (File.Exists(portDir)) return portDir;

                // 3. Linux / Unix ~/.config paths
                string userHome = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                string linuxPath = Path.Combine(userHome, ".config", "SCP Secret Laboratory", "config", Server.Port.ToString(), fileName);
                if (File.Exists(linuxPath)) return linuxPath;

                // 4. Linux dedicated server default path (/home/scpsl-server/.config/...)
                string dedicatedLinux = Path.Combine("/home/scpsl-server/.config", "SCP Secret Laboratory", "config", Server.Port.ToString(), fileName);
                if (File.Exists(dedicatedLinux)) return dedicatedLinux;

                // 5. Global template config directories
                string globalDir = Path.Combine(appData, "SCP Secret Laboratory", "config", "config_template", fileName);
                if (File.Exists(globalDir)) return globalDir;

                string linuxGlobal = Path.Combine(userHome, ".config", "SCP Secret Laboratory", "config", "config_template", fileName);
                if (File.Exists(linuxGlobal)) return linuxGlobal;

                // Fallback: return portDir so WriteConfigFile can create it if needed
                return portDir;
            }
            catch
            {
                return fileName;
            }
        }

        private string GetPluginsJson()
        {
            try
            {
                var list = new List<string>();

                // 1. Iterate LabAPI loaded plugins
                try
                {
                    var pluginsDict = LabApi.Loader.PluginLoader.Plugins;
                    var enabledSet = LabApi.Loader.PluginLoader.EnabledPlugins;

                    if (pluginsDict != null)
                    {
                        foreach (var kvp in pluginsDict)
                        {
                            var p = kvp.Key;
                            if (p == null) continue;

                            string name = EscapeJson(p.Name ?? "Unknown");
                            string version = EscapeJson(p.Version != null ? p.Version.ToString() : "1.0.0");
                            string author = EscapeJson(p.Author ?? "Unknown");
                            string desc = EscapeJson(p.Description ?? "");
                            bool isEnabled = enabledSet != null && enabledSet.Contains(p);

                            list.Add($"{{\"name\":\"{name}\",\"version\":\"{version}\",\"author\":\"{author}\",\"description\":\"{desc}\",\"enabled\":{(isEnabled ? "true" : "false")},\"type\":\"Plugin\"}}");
                        }
                    }
                }
                catch (Exception pEx)
                {
                    Logger.Warn($"[PluginWS] Could not enumerate LabAPI Plugins directly: {pEx.Message}");
                }

                // Fallback / Supplement: If list is empty, scan LabApi.Loader.Features.Paths.PathManager.Plugins directory
                if (list.Count == 0)
                {
                    try
                    {
                        var pluginsDir = LabApi.Loader.Features.Paths.PathManager.Plugins;
                        if (pluginsDir != null && pluginsDir.Exists)
                        {
                            foreach (var file in pluginsDir.GetFiles("*.dll", SearchOption.AllDirectories))
                            {
                                string fName = EscapeJson(Path.GetFileNameWithoutExtension(file.Name));
                                list.Add($"{{\"name\":\"{fName}\",\"version\":\"1.0.0\",\"author\":\"External\",\"description\":\"Installed Plugin DLL\",\"enabled\":true,\"type\":\"Plugin\"}}");
                            }
                        }
                    }
                    catch { }
                }

                // 2. Iterate Dependencies
                try
                {
                    var deps = LabApi.Loader.PluginLoader.Dependencies;
                    if (deps != null)
                    {
                        foreach (var asm in deps)
                        {
                            if (asm == null) continue;
                            string name = EscapeJson(asm.GetName().Name ?? "Dependency");
                            string version = EscapeJson(asm.GetName().Version != null ? asm.GetName().Version.ToString() : "1.0.0");
                            list.Add($"{{\"name\":\"{name}\",\"version\":\"{version}\",\"author\":\"LabAPI Dep\",\"description\":\"Assembly Dependency\",\"enabled\":true,\"type\":\"Dependency\"}}");
                        }
                    }
                }
                catch { }

                return "[" + string.Join(",", list) + "]";
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Failed to serialize plugins: {ex.Message}");
                return "[]";
            }
        }

        private struct StatusSnapshot
        {
            public double Tps;
            public int PlayerCount;
            public int MaxPlayers;
            public ushort Port;
        }

        private struct PlayerSnapshot
        {
            public string Id;
            public string Name;
            public string Role;
            public float Hp;
            public bool IsMuted;
        }

        private class CachedProcMem
        {
            public long MemoryBytes;
            public long TotalSystemBytes;
            public long FreeSystemBytes;
            public long UptimeSec;
            public DateTime CachedAt;
        }

        private static CachedProcMem _procMemCache;
        private static readonly object _procMemLock = new object();

        private static CachedProcMem GetProcMemStats()
        {
            lock (_procMemLock)
            {
                if (_procMemCache != null && (DateTime.UtcNow - _procMemCache.CachedAt).TotalSeconds < 2.0)
                {
                    return _procMemCache;
                }

                var stats = new CachedProcMem { CachedAt = DateTime.UtcNow };
                try
                {
                    using (var proc = Process.GetCurrentProcess())
                    {
                        stats.MemoryBytes = proc.WorkingSet64;
                        try
                        {
                            stats.UptimeSec = (long)(DateTime.Now - proc.StartTime).TotalSeconds;
                        }
                        catch { }
                    }
                }
                catch { }

                // Linux fallback for uptime if proc.StartTime failed
                try
                {
                    if (stats.UptimeSec <= 0 && File.Exists("/proc/uptime"))
                    {
                        string firstLine = File.ReadLines("/proc/uptime").FirstOrDefault();
                        if (!string.IsNullOrEmpty(firstLine))
                        {
                            string[] p = firstLine.Split(' ');
                            if (p.Length > 0 && double.TryParse(p[0], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double upSec))
                            {
                                stats.UptimeSec = (long)upSec;
                            }
                        }
                    }
                }
                catch { }

                // Linux /proc telemetry checks
                try
                {
                    if (File.Exists("/proc/self/status"))
                    {
                        foreach (string line in File.ReadLines("/proc/self/status"))
                        {
                            if (line.StartsWith("VmRSS:", StringComparison.OrdinalIgnoreCase))
                            {
                                string[] parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length >= 2 && long.TryParse(parts[1], out long vmrssKb))
                                {
                                    stats.MemoryBytes = vmrssKb * 1024;
                                }
                                break;
                            }
                        }
                    }

                    if (File.Exists("/proc/meminfo"))
                    {
                        foreach (string line in File.ReadLines("/proc/meminfo"))
                        {
                            if (line.StartsWith("MemTotal:", StringComparison.OrdinalIgnoreCase))
                            {
                                string[] parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length >= 2 && long.TryParse(parts[1], out long totalKb))
                                {
                                    stats.TotalSystemBytes = totalKb * 1024;
                                }
                            }
                            else if (line.StartsWith("MemAvailable:", StringComparison.OrdinalIgnoreCase) ||
                                     (stats.FreeSystemBytes == 0 && line.StartsWith("MemFree:", StringComparison.OrdinalIgnoreCase)))
                            {
                                string[] parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length >= 2 && long.TryParse(parts[1], out long freeKb))
                                {
                                    stats.FreeSystemBytes = freeKb * 1024;
                                }
                            }
                        }
                    }
                }
                catch { }

                _procMemCache = stats;
                return stats;
            }
        }

        private static string BuildServerStatusJson(StatusSnapshot mainSnap)
        {
            try
            {
                var proc = GetProcMemStats();
                long memoryMb = proc.MemoryBytes > 0 ? proc.MemoryBytes / (1024 * 1024) : 0;

                return "{" +
                    $"\"memoryMb\":{memoryMb}," +
                    $"\"memoryBytes\":{proc.MemoryBytes}," +
                    $"\"totalSystemBytes\":{proc.TotalSystemBytes}," +
                    $"\"freeSystemBytes\":{proc.FreeSystemBytes}," +
                    $"\"uptimeSeconds\":{proc.UptimeSec}," +
                    $"\"playerCount\":{mainSnap.PlayerCount}," +
                    $"\"maxPlayers\":{mainSnap.MaxPlayers}," +
                    $"\"port\":{mainSnap.Port}," +
                    $"\"tps\":{Math.Round(mainSnap.Tps, 1).ToString(System.Globalization.CultureInfo.InvariantCulture)}" +
                "}";
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Failed to get server status: {ex.Message}");
                return "{}";
            }
        }

        private Task<T> RunOnMainThreadAsync<T>(Func<T> func)
        {
            if (_syncContext == null) return Task.FromResult(func());
            var tcs = new TaskCompletionSource<T>();
            _syncContext.Post(_ =>
            {
                try { tcs.SetResult(func()); }
                catch (Exception ex) { tcs.SetException(ex); }
            }, null);
            return tcs.Task;
        }

        private static string SerializePlayers(PlayerSnapshot[] players)
        {
            try
            {
                var sb = new StringBuilder("[", Math.Max(32, players.Length * 128));
                bool first = true;
                for (int i = 0; i < players.Length; i++)
                {
                    var p = players[i];
                    string rawId = p.Id;
                    string rawName = p.Name;
                    if (string.IsNullOrWhiteSpace(rawId) || rawId == "Unknown" || string.IsNullOrWhiteSpace(rawName) || rawName == "(null)" || rawName == "null") continue;
                    if (rawName.IndexOf("dedicated", StringComparison.OrdinalIgnoreCase) >= 0 || rawId.StartsWith("dedicated", StringComparison.OrdinalIgnoreCase)) continue;

                    if (!first) sb.Append(',');
                    first = false;

                    sb.Append("{\"id\":\"");
                    sb.Append(EscapeJson(rawId));
                    sb.Append("\",\"name\":\"");
                    sb.Append(EscapeJson(rawName));
                    sb.Append("\",\"role\":\"");
                    sb.Append(EscapeJson(p.Role));
                    sb.Append("\",\"hp\":");
                    sb.Append(p.Hp.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    sb.Append(",\"isMuted\":");
                    sb.Append(p.IsMuted ? "true" : "false");
                    sb.Append('}');
                }
                sb.Append(']');
                return sb.ToString();
            }
            catch (Exception ex)
            {
                Logger.Error($"[PluginWS] Failed to serialize players: {ex.Message}");
                return "[]";
            }
        }

        private static string ExtractField(string json, string field)
        {
            if (string.IsNullOrEmpty(json) || string.IsNullOrEmpty(field)) return "";
            try
            {
                int len = json.Length;
                int depth = 0;
                int i = 0;

                while (i < len)
                {
                    char c = json[i];
                    if (c == '"')
                    {
                        int strStart = ++i;
                        bool escaped = false;
                        while (i < len)
                        {
                            char sc = json[i];
                            if (escaped)
                            {
                                escaped = false;
                            }
                            else if (sc == '\\')
                            {
                                escaped = true;
                            }
                            else if (sc == '"')
                            {
                                break;
                            }
                            i++;
                        }
                        int strLen = i - strStart;
                        i++; // skip closing quote

                        // Only match keys at the root object level (depth == 1)
                        if (depth == 1)
                        {
                            while (i < len && char.IsWhiteSpace(json[i])) i++;
                            if (i < len && json[i] == ':')
                            {
                                i++; // skip colon
                                while (i < len && char.IsWhiteSpace(json[i])) i++;

                                if (strLen == field.Length && string.CompareOrdinal(json, strStart, field, 0, strLen) == 0)
                                {
                                    if (i < len && json[i] == '"')
                                    {
                                        i++; // skip opening quote
                                        var sb = new StringBuilder();
                                        bool valEscaped = false;
                                        while (i < len)
                                        {
                                            char vc = json[i];
                                            if (valEscaped)
                                            {
                                                switch (vc)
                                                {
                                                    case '"': sb.Append('"'); break;
                                                    case '\\': sb.Append('\\'); break;
                                                    case '/': sb.Append('/'); break;
                                                    case 'b': sb.Append('\b'); break;
                                                    case 'f': sb.Append('\f'); break;
                                                    case 'n': sb.Append('\n'); break;
                                                    case 'r': sb.Append('\r'); break;
                                                    case 't': sb.Append('\t'); break;
                                                    default: sb.Append(vc); break;
                                                }
                                                valEscaped = false;
                                            }
                                            else if (vc == '\\')
                                            {
                                                valEscaped = true;
                                            }
                                            else if (vc == '"')
                                            {
                                                return sb.ToString();
                                            }
                                            else
                                            {
                                                sb.Append(vc);
                                            }
                                            i++;
                                        }
                                        return sb.ToString();
                                    }
                                    else
                                    {
                                        int valStart = i;
                                        while (i < len && json[i] != ',' && json[i] != '}' && json[i] != ']' && !char.IsWhiteSpace(json[i]))
                                        {
                                            i++;
                                        }
                                        return json.Substring(valStart, i - valStart).Trim();
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    if (c == '{' || c == '[') depth++;
                    else if (c == '}' || c == ']') depth--;

                    i++;
                }

                return "";
            }
            catch
            {
                return "";
            }
        }

        private static bool NeedsJsonEscape(string str)
        {
            for (int i = 0; i < str.Length; i++)
            {
                char c = str[i];
                if (c < 32 || c == '"' || c == '\\') return true;
            }
            return false;
        }

        private static string EscapeJson(string str)
        {
            if (string.IsNullOrEmpty(str)) return "";
            if (!NeedsJsonEscape(str)) return str;

            var sb = new StringBuilder(str.Length + 16);
            for (int i = 0; i < str.Length; i++)
            {
                char c = str[i];
                switch (c)
                {
                    case '\\': sb.Append("\\\\"); break;
                    case '"': sb.Append("\\\""); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    default:
                        if (c < 32)
                            sb.AppendFormat("\\u{0:X4}", (int)c);
                        else
                            sb.Append(c);
                        break;
                }
            }
            return sb.ToString();
        }

        private static string SanitizeConsoleArgument(string input, int maxLength = 120)
        {
            if (string.IsNullOrEmpty(input)) return "";
            string clean = Regex.Replace(input, @"[\r\n;\x00-\x1F]+", " ");
            clean = Regex.Replace(clean, @"\s+", " ").Trim();
            if (clean.Length > maxLength) clean = clean.Substring(0, maxLength);
            return clean;
        }

        private static string SanitizeTargetId(string targetId)
        {
            if (string.IsNullOrEmpty(targetId)) return "";
            return Regex.Replace(targetId, @"[^a-zA-Z0-9@._-]", "");
        }
    }
}

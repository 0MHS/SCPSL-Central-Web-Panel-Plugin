using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LabApi.Events.Arguments.PlayerEvents;
using LabApi.Events.Arguments.ServerEvents;
using LabApi.Events.Handlers;
using LabApi.Features.Console;
using LabApi.Features.Wrappers;
using LabApi.Loader.Features.Plugins;

namespace ScpWebPanelPlugin
{
    public class WebPanelPlugin : Plugin<Config>
    {
        public override string Name => "WebPanelPlugin";
        public override string Description => "Web Panel Reverse WebSocket & Telemetry Client";
        public override string Author => "MHS";
        public override Version Version => new Version(2, 1, 1);
        public override Version RequiredApiVersion => new Version(1, 0, 0);

        public PluginWebSocketClient WsClient { get; private set; }
        private SynchronizationContext _syncContext;

        public override void Enable()
        {
            try
            {
                Logger.Info("WebPanelPlugin (v2.1.1 High-Performance SaaS Client) loading via LabAPI...");

                // Capture main thread synchronization context
                _syncContext = SynchronizationContext.Current;

                // Start Reverse WebSocket Client
                WsClient = new PluginWebSocketClient(Config, _syncContext);
                WsClient.Start();

                // Register round telemetry hooks if enabled
                if (Config == null || Config.ReportRoundStats)
                {
                    ServerEvents.RoundStarted += OnRoundStarted;
                    ServerEvents.RoundEnded += OnRoundEnded;
                    Logger.Info("WebPanelPlugin round statistics reporting enabled.");
                }

                try
                {
                    ServerEvents.CommandExecuted += OnCommandExecuted;
                    Logger.Info("WebPanelPlugin CommandExecuted event hooked.");
                }
                catch { }

                // Register in-game player hooks
                PlayerEvents.Left += OnPlayerLeft;
                if (Config == null || Config.ReportInGameReports)
                {
                    PlayerEvents.ReportedPlayer += OnReportedPlayer;
                    PlayerEvents.ReportedCheater += OnReportedCheater;
                    Logger.Info("WebPanelPlugin in-game reports forwarding enabled.");
                }

                // Register console log streaming
                if (Config == null || Config.StreamConsoleLogs)
                {
                    UnityEngine.Application.logMessageReceivedThreaded += OnUnityLogMessage;
                    Logger.Info("WebPanelPlugin console log streaming hooked to Unity logMessageReceivedThreaded.");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"WebPanelPlugin failed to start: {ex}");
            }
        }

        public override void Disable()
        {
            try
            {
                UnityEngine.Application.logMessageReceivedThreaded -= OnUnityLogMessage;
            }
            catch { }

            try
            {
                ServerEvents.RoundStarted -= OnRoundStarted;
                ServerEvents.RoundEnded -= OnRoundEnded;
                ServerEvents.CommandExecuted -= OnCommandExecuted;
            }
            catch { }

            try
            {
                PlayerEvents.Left -= OnPlayerLeft;
                PlayerEvents.ReportedPlayer -= OnReportedPlayer;
                PlayerEvents.ReportedCheater -= OnReportedCheater;
            }
            catch { }

            try
            {
                WsClient?.Stop();
                WsClient = null;
            }
            catch { }
        }

        private void OnUnityLogMessage(string condition, string stackTrace, UnityEngine.LogType type)
        {
            try
            {
                if (WsClient == null || !WsClient.IsConnected) return;
                if (string.IsNullOrWhiteSpace(condition)) return;

                // 1. Filter out internal Unity GUI, Audio, and scene noise that floods the console
                if (condition.Contains("missing script") ||
                    condition.Contains("ButtonAudio") ||
                    condition.Contains("ValueButton") ||
                    condition.Contains("OperationalGuide") ||
                    condition.Contains("[T11]") ||
                    condition.Contains("GUI") ||
                    condition.Contains("AudioSource") ||
                    condition.Contains("Canvas") ||
                    condition.Contains("Font") ||
                    condition.Contains("MeshRenderer") ||
                    condition.Contains("The referenced script on this Behaviour"))
                {
                    return;
                }

                // 2. Format with timestamp and log type prefix if informative
                string prefix = type == UnityEngine.LogType.Error || type == UnityEngine.LogType.Exception ? "[ERROR] " :
                               type == UnityEngine.LogType.Warning ? "[WARN] " : "";
                string formatted = $"{prefix}{condition}";

                _ = WsClient.SendConsoleLogAsync(formatted);
            }
            catch { }
        }

        private void OnReportedPlayer(PlayerReportedPlayerEventArgs ev)
        {
            SendReportToWs(ev.Player, ev.Target, ev.Reason, "Rule Breaking");
        }

        private void OnReportedCheater(PlayerReportedCheaterEventArgs ev)
        {
            SendReportToWs(ev.Player, ev.Target, ev.Reason, "Cheating / Exploiting");
        }

        private void SendReportToWs(Player reporter, Player target, string reason, string category)
        {
            try
            {
                if (Config != null && !Config.ReportInGameReports) return;
                if (WsClient == null || !WsClient.IsConnected) return;

                string reporterId = EscapeJson(reporter?.UserId ?? "Unknown");
                string reporterName = EscapeJson(reporter?.Nickname ?? "Unknown");
                string reporterRole = EscapeJson(reporter?.Role.ToString() ?? "None");
                int reporterNetId = reporter?.PlayerId ?? 0;

                string reportedId = EscapeJson(target?.UserId ?? "Unknown");
                string reportedName = EscapeJson(target?.Nickname ?? "Unknown");
                string reportedRole = EscapeJson(target?.Role.ToString() ?? "None");
                int reportedNetId = target?.PlayerId ?? 0;

                string safeReason = EscapeJson(reason ?? "No reason provided");
                string safeCategory = EscapeJson(category);
                string serverInfo = $"Server (Port {Server.Port})";
                int roundDuration = (int)Round.Duration.TotalSeconds;

                string json = "{" +
                    $"\"reporterId\":\"{reporterId}\"," +
                    $"\"reporterName\":\"{reporterName}\"," +
                    $"\"reporterRole\":\"{reporterRole}\"," +
                    $"\"reporterNetId\":{reporterNetId}," +
                    $"\"reportedId\":\"{reportedId}\"," +
                    $"\"reportedName\":\"{reportedName}\"," +
                    $"\"reportedRole\":\"{reportedRole}\"," +
                    $"\"reportedNetId\":{reportedNetId}," +
                    $"\"reason\":\"{safeReason}\"," +
                    $"\"category\":\"{safeCategory}\"," +
                    $"\"server\":\"{serverInfo}\"," +
                    $"\"roundDuration\":{roundDuration}" +
                "}";

                Logger.Info($"[WebPanelPlugin] Player report: {reporterName} reported {reportedName} for {category}. Forwarding over WebSocket...");
                _ = WsClient.SendEventAsync("player_report", json);
            }
            catch (Exception ex)
            {
                Logger.Error($"[WebPanelPlugin] Error in SendReportToWs: {ex.Message}");
            }
        }

        private void OnRoundEnded(RoundEndedEventArgs ev)
        {
            try
            {
                if (Config != null && !Config.ReportRoundStats) return;
                if (WsClient == null || !WsClient.IsConnected) return;

                string winner = "Draw";
                string teamStr = ev.LeadingTeam.ToString();
                switch (teamStr)
                {
                    case "FacilityForces":
                        winner = "MTF";
                        break;
                    case "ChaosInsurgency":
                        winner = "ClassD";
                        break;
                    case "Anomalies":
                        winner = "SCPs";
                        break;
                    case "Draw":
                        winner = "Draw";
                        break;
                    default:
                        winner = teamStr;
                        break;
                }

                int duration = (int)Round.Duration.TotalSeconds;
                int scpCount = Round.SurvivingSCPs;
                int humanCount = Player.List.Count;
                int escapedDClass = Round.EscapedClassD;
                int escapedScientists = Round.EscapedScientists;
                int kills = Round.TotalDeaths;

                string json = "{" +
                    $"\"winner\":\"{winner}\"," +
                    $"\"duration\":{duration}," +
                    $"\"scpCount\":{scpCount}," +
                    $"\"humanCount\":{humanCount}," +
                    $"\"escapedDClass\":{escapedDClass}," +
                    $"\"escapedScientists\":{escapedScientists}," +
                    $"\"kills\":{kills}" +
                "}";

                Logger.Info($"[WebPanelPlugin] Round ended. Reporting winner: {winner}, duration: {duration}s over WebSocket...");
                _ = WsClient.SendEventAsync("round_ended", json);
            }
            catch (Exception ex)
            {
                Logger.Error($"[WebPanelPlugin] Error in OnRoundEnded handler: {ex.Message}");
            }
        }

        private void OnPlayerLeft(PlayerLeftEventArgs ev)
        {
            try
            {
                if (ev?.Player == null) return;
                if (WsClient == null || !WsClient.IsConnected) return;

                string pName = ev.Player.Nickname ?? "";
                string pId = ev.Player.UserId ?? "";
                string pRole = ev.Player.Role.ToString();

                // Discard invalid, dummy, null, or dedicated server instances
                if (string.IsNullOrWhiteSpace(pId) || pId == "Unknown" || string.IsNullOrWhiteSpace(pName) || pName == "(null)" || pName == "null") return;
                if (pName.ToLower().Contains("dedicated") || pId.ToLower().StartsWith("dedicated")) return;

                string json = "{" +
                    $"\"id\":\"{EscapeJson(pId)}\"," +
                    $"\"name\":\"{EscapeJson(pName)}\"," +
                    $"\"role\":\"{EscapeJson(pRole)}\"," +
                    $"\"leftAt\":\"{DateTime.UtcNow:O}\"" +
                "}";

                _ = WsClient.SendEventAsync("player_left", json);
            }
            catch (Exception ex)
            {
                Logger.Error($"[WebPanelPlugin] Error in OnPlayerLeft: {ex.Message}");
            }
        }

        private void OnRoundStarted()
        {
            try
            {
                // Main-thread safe snapshot of SCP team roster immediately after team assignment
                var scpSnapshots = new System.Collections.Generic.List<ScpPlayerSnapshot>();

                try
                {
                    if (Player.List != null)
                    {
                        foreach (var p in Player.List)
                        {
                            if (p == null) continue;
                            string roleStr = p.Role.ToString();

                            if (roleStr.StartsWith("Scp") && !roleStr.Equals("Scp0492", StringComparison.OrdinalIgnoreCase))
                            {
                                scpSnapshots.Add(new ScpPlayerSnapshot
                                {
                                    Name = p.Nickname ?? "Unknown",
                                    UserId = p.UserId ?? "",
                                    Role = roleStr
                                });
                            }
                        }
                    }
                }
                catch (Exception snapEx)
                {
                    Logger.Warn($"[WebPanelPlugin] Failed to snapshot SCP roster directly: {snapEx.Message}");
                }

                // Offload JSON serialization and WebSocket transmission asynchronously off the main Unity thread
                Task.Run(async () =>
                {
                    try
                    {
                        if (WsClient == null || !WsClient.IsConnected) return;

                        var scpList = new System.Collections.Generic.List<string>();
                        foreach (var item in scpSnapshots)
                        {
                            string safeName = EscapeJson(item.Name);
                            string safeId = EscapeJson(item.UserId);
                            string safeRole = EscapeJson(item.Role);
                            scpList.Add($"{{\"name\":\"{safeName}\",\"id\":\"{safeId}\",\"role\":\"{safeRole}\"}}");
                        }

                        string scpsJson = "[" + string.Join(",", scpList) + "]";
                        string payload = "{" +
                            $"\"timestamp\":\"{DateTime.UtcNow:O}\"," +
                            $"\"scps\":{scpsJson}" +
                        "}";

                        await WsClient.SendEventAsync("round_started", payload);
                        Logger.Info($"[WebPanelPlugin] Round started. Forwarded initial SCP roster over WS ({scpSnapshots.Count} SCPs).");
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"[WebPanelPlugin] Error serializing/sending OnRoundStarted payload: {ex.Message}");
                    }
                });
            }
            catch (Exception ex)
            {
                Logger.Error($"[WebPanelPlugin] Error in OnRoundStarted: {ex.Message}");
            }
        }

        private struct ScpPlayerSnapshot
        {
            public string Name;
            public string UserId;
            public string Role;
        }

        private void OnCommandExecuted(CommandExecutedEventArgs ev)
        {
            try
            {
                if (WsClient == null || !WsClient.IsConnected) return;
                string resp = ev?.Response;
                if (!string.IsNullOrWhiteSpace(resp))
                {
                    _ = WsClient.SendConsoleLogAsync(resp);
                }
            }
            catch { }
        }

        private static string EscapeJson(string str)
        {
            if (string.IsNullOrEmpty(str)) return "";
            return str.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r");
        }
    }
}

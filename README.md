# SCP:SL Web Panel — LabAPI Game Server Plugin (v2.1.1)

[![Release](https://img.shields.io/badge/Release-v2.1.1-brightgreen.svg)](https://github.com/mhsbaba/SCPSL-Central-Web-Panel-Plugin/releases)
[![Target](https://img.shields.io/badge/.NET%20Framework-4.8-blue.svg)](https://dotnet.microsoft.com/)
[![Modding](https://img.shields.io/badge/Framework-LabAPI-orange.svg)](https://github.com/CedModV2/LabAPI)
[![Status](https://img.shields.io/badge/Live%20Panel-Online-success.svg)](https://scpslwebpanel.zapto.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Official LabAPI game server plugin and C# reference source for the **SCP: Secret Laboratory Central Web Panel** ([https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org)).

This plugin establishes an outbound secure TLS Reverse WebSocket tunnel from your dedicated game server directly to the Central Web Panel, allowing real-time browser-based moderation, live console streaming, player management, and telemetry **with zero port-forwarding or firewall changes**.

---

## 🚀 Key Features
- **Zero Inbound Port Forwarding**: The plugin initiates an outbound secure TLS WebSocket connection (`ClientWebSocket`) to `wss://scpslwebpanel.zapto.org/ws/plugin`. You never need to open inbound ports or expose game ports to the public internet.
- **Zero-Secret Public Binary**: Compiled with release hardening, deterministic metadata, and stripped debug symbols.
- **High Performance & Main-Thread Safety**: All game interactions (`p.Kick()`, `p.Ban()`, `VoiceChatMutes`, `ServerConsole`) are dispatched asynchronously to Unity's main thread via captured synchronization contexts.
- **Outbound Bounded Queue**: Prevents thread contention and memory leaks during high tickrate bursts.
- **Real-Time Telemetry & Console Streaming**: Streams filtered server logs, round statistics, and in-game reports instantly to your team's panel.
- **Granular Staff Delegation**: Server owners can invite moderators by Discord ID through the panel with customized permissions without sharing server credentials.
- **Open-Source Transparency**: Full C# source code included in `src/` for complete auditability.

---

## 📦 Installation & Setup Guide

### Prerequisites
- An **SCP: Secret Laboratory Dedicated Server** (Windows or Linux).
- **LabAPI** (v1.0.0+) installed on your server.

### Step 1: Download & Place the Plugin DLL
1. Download `ScpWebPanelPlugin.dll` from the latest GitHub Release or your Central Web Panel dashboard.
2. Place the file into your game server's global plugin directory:
   - **Linux**: `/home/<user>/.config/SCP Secret Laboratory/LabAPI/plugins/global/ScpWebPanelPlugin.dll`
   - **Windows**: `%appdata%\SCP Secret Laboratory\LabAPI\plugins\global\ScpWebPanelPlugin.dll`

### Step 2: Register on the Central Web Panel
1. Open [https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org) in your browser.
2. Sign in with your **Discord account**.
3. Under the server management dropdown, click **Add Server** (or use your default assigned server).
4. Copy your unique **Server Connection Token** (`scp_...`).

### Step 3: Configure `config.yml`
Open your server port configuration file:
- **Linux**: `/home/<user>/.config/SCP Secret Laboratory/LabAPI/configs/<port>/WebPanelPlugin/config.yml`
- **Windows**: `%appdata%\SCP Secret Laboratory\LabAPI\configs\<port>\WebPanelPlugin\config.yml`

*(If the file does not exist yet, you can create it or start the server once so LabAPI generates the template).*

Paste your configuration:
```yaml
web_panel_plugin:
  is_enabled: true
  central_ws_url: "wss://scpslwebpanel.zapto.org/ws/plugin"
  server_token: "YOUR_SERVER_TOKEN_HERE"
  server_name: "My Awesome SCP:SL Server"
  reconnect_interval_seconds: 5
  max_reconnect_interval_seconds: 60
  stream_console_logs: true
  report_round_stats: true
  report_in_game_reports: true
```

### Step 4: Start or Reload Your Server
Restart your game server or run `reload plugins` in your server console.
Once connected, you will see the following line in your server console:
```text
[INFO] [ScpWebPanelPlugin] [PluginWS] Connected successfully! Sending authentication handshake...
```
In your browser on [https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org), your server status indicator will switch to green (**ONLINE / TUNNEL ACTIVE**), and live player rosters, console logs, and moderation controls will become active immediately!

---

## 🔒 Security & Privacy
- **Encrypted Transmission**: All communication between your game server and the central panel is encrypted via TLS 1.3 / WSS.
- **Tenancy Isolation**: Each game server is strictly bound to its owner's Discord account and delegated staff.
- **Binary Integrity**: Built with strict argument sanitization protecting `UserIdBans.txt` and preventing remote command injection.

---

## 🛠 Support & Inquiries
- **Central Web Panel**: [https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org)
- For issues, feature requests, or bug reports, please open an issue on this repository.

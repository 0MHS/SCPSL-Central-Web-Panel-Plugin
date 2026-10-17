using System.ComponentModel;

namespace ScpWebPanelPlugin
{
    public class Config
    {
        [Description("Is the plugin enabled?")]
        public bool IsEnabled { get; set; } = true;

        [Description("The WebSocket URL of the Central Web Panel Backend (e.g., ws://your-panel.com:3001/ws/plugin or wss://panel.example.com/ws/plugin)")]
        public string CentralWsUrl { get; set; } = "wss://scpslwebpanel.zapto.org/ws/plugin";

        [Description("Unique Server Authentication Token provided by the Central Web Panel.")]
        public string ServerToken { get; set; } = "DEFAULT_SERVER_TOKEN";

        [Description("Custom display name for this game server in the Web Panel.")]
        public string ServerName { get; set; } = "SCP:SL Dedicated Server";

        [Description("Base reconnect interval in seconds when WebSocket disconnects.")]
        public int ReconnectIntervalSeconds { get; set; } = 5;

        [Description("Maximum reconnect backoff interval in seconds.")]
        public int MaxReconnectIntervalSeconds { get; set; } = 60;

        [Description("Whether to stream server console log output to the Web Panel in real-time.")]
        public bool StreamConsoleLogs { get; set; } = true;

        [Description("Whether to automatically report round statistics to the Web Panel when a round ends.")]
        public bool ReportRoundStats { get; set; } = true;

        [Description("Whether to automatically forward in-game player reports to the Web Panel.")]
        public bool ReportInGameReports { get; set; } = true;
    }
}

# SCP:SL Central Web Panel — LabAPI Game Server Plugin (v2.2.2)

[![Release](https://img.shields.io/badge/Release-v2.2.2-brightgreen.svg)](https://github.com/0MHS/SCPSL-Central-Web-Panel-Plugin/releases)
[![Target](https://img.shields.io/badge/.NET%20Framework-4.8-blue.svg)](https://dotnet.microsoft.com/)
[![Modding](https://img.shields.io/badge/Framework-LabAPI-orange.svg)](https://github.com/CedModV2/LabAPI)
[![Status](https://img.shields.io/badge/Live%20Panel-Online-success.svg)](https://scpslwebpanel.zapto.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> [!NOTE]
> **Disclaimer & Non-Affiliation Notice**: This project is an independent community-developed open-source tool and is **NOT affiliated with, endorsed by, or sponsored by Northwood Studios**. *SCP: Secret Laboratory* and related game assets and marks are the property of Northwood Studios and the SCP Wiki community (licensed under CC BY-SA 3.0).

Modern, zero-configuration cloud administrative, moderation, and telemetry web panel for **SCP: Secret Laboratory** dedicated servers (powered by **LabAPI**). 

Connect your game server to the central panel ([https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org)) simply by dropping `ScpWebPanelPlugin.dll` into your plugins folder and pasting your unique token.

![SCP:SL Central Web Panel Dashboard](docs/img/dashboard.png)

---

## ⚡ Key Highlights & Server Owner Benefits

### 🚀 Sıfır Port Açma & Zahmetsiz Kurulum (Zero Port Forwarding)
- **Modem, Güvenlik Duvarı veya Statik IP Derdi Yok**: Eklenti, oyun sunucunuzdan merkezi panele güvenli bir ters WebSocket tüneli (`wss://scpslwebpanel.zapto.org/ws/plugin`) açar.
- **NAT & Dinamik IP Dostu**: Sunucunuz ister ev bilgisayarınızda ister kurumsal bir VDS üzerinde olsun; hiçbir port açmadan, Linux daemon veya tmux konfigürasyonlarıyla uğraşmadan tek tıkla bağlanır.
- **Otomatik Yeniden Bağlanma**: İnternet kesintilerinde oyun sunucusunu yormayan akıllı üstel geri çekilme (exponential backoff) algoritmasıyla otomatik olarak tekrar hatta girer.

---

### 💻 Canlı Oyun Konsolu, Log Akışı & CASSIE Anonsları
Web tarayıcınız üzerinden doğrudan oyun sunucunuzun canlı terminalini yönetin:

![Canlı Terminal ve Konsol Akışı](docs/img/terminal.png)

- **Çift Yönlü Canlı Terminal**: Uzaktan komut gönderin, oyun motoru ve Remote Admin (RA) yanıtlarını milisaniyelik gecikmeyle anında görüntüleyin.
- **CASSIE Sesli Anons & Ekran Bildirimi**: Tesis içine doğrudan CASSIE sesli anonsları (`cassie`) veya oyuncuların ekranına anlık uyarı bildirimleri (`bc`) yollayın.
- **Hızlı Komut & Kısayollar**: Tek tıkla konfigürasyon yenileme (`reload config`), eklenti yenileme ve güvenli sunucu yeniden başlatma kontrolleri.

---

### 🔍 Teaming & Hile Tespiti (Steam İstihbarat Grafiği)
Kural ihlali yapanları ve oyun içi adaleti bozan gizli ekipleri anında yakalayın:
- **Karşılıklı Arkadaşlık Kümesi Analizi**: Aktif oyuncular arasındaki Steam arkadaşlık bağlarını graf teorisi (connected components) ile analiz ederek aynı sunucuda gizlice takım oluşturanları (Cross-Teaming) renk kodlu olarak tespit eder.
- **Steam Profil ve Saat Doğrulaması**: Oyuncuların toplam SCP:SL oynama saatlerini ve gizli profil durumlarını tarayıcı tabanlı analizle listeler.
- **Geçmiş Oyuncular Hafızası**: Sunucudan ayrılan (alt+f4 çeken veya kaçan) kural ihlalcilerini dahi sonradan kolayca cezalandırabilmeniz için yakın zamanda çıkan oyuncuları hafızada tutar.

---

### 🛡️ Görsel Envanter & Oyuncu Yönetimi
Oyuncularınızı salt metin komutlarla değil, sezgisel bir arayüzle denetleyin:
- **Görsel Envanter Yönetimi**: Oyuncuların envanterindeki eşyaları modern WebP ikonlarıyla inceleyin; tek tıkla eşya verin, silin veya can/rol değerlerini düzenleyin.
- **Hızlı Moderasyon Araçları**: Tek tıkla Kick, önceden tanımlı hazır süre butonlarıyla (10dk, 1sa, 1gün, 7gün, 30gün) Geçici Ban veya onay pencereli Kalıcı Ban uygulayın.
- **Canlı Geri Sayımlı Ses Susturma (Mute)**: Sesli sohbet cezalarını gerçek zamanlı kalan süre sayacıyla takip edin; süre bittiğinde otomatik kalkan kalıcı müteleri yönetin.
- **Oyun İçi F1 Rapor Masası**: Oyuncuların oyun içinden gönderdiği şikayetleri anında inceleyin, gerekçeleri okuyun ve doğrudan şikayet üzerinden işlem yapın.

---

### 🧩 LabAPI Eklenti & Konfigürasyon Yöneticisi
Sunucunuzdaki eklenti ekosistemini doğrudan panelden denetleyin:

![LabAPI Eklenti Yöneticisi](docs/img/pluginmanager.png)

- **Kurulu Eklentileri Görüntüleme**: Sunucuda yüklü LabAPI eklentilerini, sürüm numaralarını ve çalışma durumlarını listeler.
- **Web Tabanlı Konfigürasyon Düzenleme**: Sunucu dosyalarını açmadan eklenti `config.yml` ayarlarını doğrudan tarayıcıdan düzenleyin ve oyun içine uygulayın.
- **Sunucu Sahibi Güvenliği**: Ham DLL ve binary işlemleri yalnızca doğrulanmış sunucu sahibine özel izinlerle korunur.

---

## 🛠️ Hızlı Kurulum Rehberi (Quick Setup)

Sunucunuzu merkezi panele bağlamak yalnızca 2 dakikanızı alır:

```
[1. Panele Giriş Yap] ➔ [2. Sunucu Ekle & Token Al] ➔ [3. DLL'i Eklentilere At] ➔ [4. config.yml'e Yapıştır] ➔ [5. Canlı Panel Aktif]
```

### 1. Eklenti DLL'ini İndirin
1. Bu depodaki [Releases](https://github.com/0MHS/SCPSL-Central-Web-Panel-Plugin/releases) sayfasından veya depodan `ScpWebPanelPlugin.dll` dosyasını indirin.
2. Dosyayı sunucunuzun global eklenti dizinine yerleştirin:
   - **Linux**: `/home/<user>/.config/SCP Secret Laboratory/LabAPI/plugins/global/ScpWebPanelPlugin.dll`
   - **Windows**: `%appdata%\SCP Secret Laboratory\LabAPI\plugins\global\ScpWebPanelPlugin.dll`

### 2. Panelden Token Oluşturun
1. [https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org) adresine gidin ve **Discord ile Giriş Yapın**.
2. **"Yeni Sunucu Bağla"** (Connect Server) butonuna tıklayın, sunucunuza bir isim verin ve oluşturulan **Server Token**'ı kopyalayın.

### 3. `config.yml` Dosyasını Ayarlayın
Sunucu portunuza ait konfigürasyon dosyasını açın:
- **Linux**: `/home/<user>/.config/SCP Secret Laboratory/LabAPI/configs/<port>/WebPanelPlugin/config.yml`
- **Windows**: `%appdata%\SCP Secret Laboratory\LabAPI\configs\<port>\WebPanelPlugin\config.yml`

Aşağıdaki şablonu kopyalayıp token bilginizi yapıştırın:
```yaml
web_panel_plugin:
  is_enabled: true
  central_ws_url: "wss://scpslwebpanel.zapto.org/ws/plugin"
  server_token: "YOUR_UNIQUE_SERVER_TOKEN"
  server_name: "EU Chaos #1"
  reconnect_interval_seconds: 5
  max_reconnect_interval_seconds: 60
  stream_console_logs: true
  report_round_stats: true
  report_in_game_reports: true
```

### 4. Sunucuyu Başlatın veya Yenileyin
Sunucunuzu başlatın veya sunucu konsolunda `reload plugins` / `reload config` komutunu çalıştırın.
Sunucu konsolunda başarı mesajını göreceksiniz:
```text
[INFO] [ScpWebPanelPlugin] [PluginWS] Connected successfully! Sending authentication handshake...
```
Web panelinde sunucu göstergeniz anında **TUNNEL ACTIVE (Yeşil)** durumuna geçecektir!

---

## 🔒 Güvenlik, Gizlilik ve İzin Ayrımı (Security Architecture)

- **Uçtan Uca TLS 1.3 Şifreleme**: Oyun sunucunuz ile merkezi panel arasındaki tüm veri trafiği güvenli WebSocket (`wss://`) ve HTTPS protokolleriyle korunur.
- **Discord Tabanlı Rol & İzin Ayrımı (RBAC)**: Sunucu sahipleri, `ServerToken` bilgisini asla başkalarıyla paylaşmadan moderatörlerine Discord kullanıcı kimlikleri üzerinden özel yetkiler (konsol izleme, ban atma, susturma vb.) tanımlayabilir.
- **Sıfır Şifre Depolama**: Sistem hiçbir kullanıcının şifresini veya hassas kimlik bilgisini barındırmaz; Discord OAuth2 `identify` mekanizmasını kullanır.

---

## ⚖️ Yasal Bildirimler & Lisans

- **Lisans**: Bu proje açık kaynaklı olup [MIT License](LICENSE) şartları altında lisanslanmıştır.
- **Northwood Studios Feragatnamesi**: Bu yazılım tamamen bağımsız bir topluluk projesidir. Northwood Studios ile hiçbir resmi bağı, sponsorluğu veya ortaklığı bulunmamaktadır. *SCP: Secret Laboratory*, Northwood Studios'un tescilli markasıdır.
- **SCP Evreni & Lisans**: SCP Foundation temaları ve kurgusu [Creative Commons Attribution-ShareAlike 3.0 (CC BY-SA 3.0)](https://creativecommons.org/licenses/by-sa/3.0/) lisansına tabidir.

---

## 🌐 Canlı Web Panel
Web paneline doğrudan erişmek için: **[https://scpslwebpanel.zapto.org](https://scpslwebpanel.zapto.org)**

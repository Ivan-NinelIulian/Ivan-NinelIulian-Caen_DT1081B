# Ivan-NinelIulian-Caen_DT1081B
Sistemul a fost conceput ca o aplicație industrială de tip **24/7/365 (High Availability)**, proiectată să funcționeze luni de zile fără intervenție umană. Acesta rezolvă problemele clasice ale automatizărilor web (blocaje Selenium, pierderi de memorie, deconectări de rețea, crash-uri silențioase ale browserului sau instabilități ale protocolului WebSocket).

---

### 1. Arhitectura de Înaltă Nivel (High-Level Architecture)

Sistemul este împărțit în **trei module complet independente**, care rulează ca procese separate la nivel de sistem de operare, dar comunică implicit prin fișiere de configurare comune și sunt orchestrate de un Manager de Procese centralizat:

1. **`supervisor_v3.py` (Orchestratorul Principal)** – Root process (Guardian) care pornește și supraveghează activitatea celorlalte două module.
2. **`caen_monitor_v6_optimized.py` (Achiziția și Interfața Grafică)** – Motorul principal de colectare a datelor prin WebSockets (fără interfață web vizibilă) și interfața GUI realizată în CustomTkinter.
3. **`refresh_page1.py` (Automatizarea Web și Mentenanța sesiunii prin Selenium)** – Driver web izolat, responsabil cu emularea comportamentului utilizatorului (Login securizat, menținerea sesiunii HTTP active, prevenirea timeout-urilor aparatului).

---

### 2. Descrierea Tehnică a Modulelor

#### A. Supervisor de Procese și Managementul Resurselor (`supervisor_v3.py`)

Acționează ca un manager de procese nativ (similar cu un mini-systemd sau Supervisor din Linux).

* **Supraveghere prin Backoff Exponential:** Rulează bucle de monitorizare asincrone (folosind `subprocess.Popen`). Dacă un script copil crashează în mod repetat într-un interval scurt, supervisorul aplică un algoritm de penalizare a timpului de așteptare (backoff), crescând intervalul de repornire până la 120 de secunde pentru a evita supraîncărcarea procesorului (CPU Spawning Storm).
* **Curățarea Agresivă a Proceselor Zombie (`psutil` Tree Kill):** Browserul Chrome generat prin Selenium deschide mai multe sub-procese (GPU Process, Network Service, Renderer). Un `proc.terminate()` standard la nivel de Python lasă adesea aceste procese în memorie (zombie). Supervisorul folosește biblioteca `psutil` pentru a mapa recursiv arborele de procese (`parent.children(recursive=True)`) și a le distruge complet prin semnale de tip `SIGKILL` la fiecare restart.
* **Izolarea și Redirecționarea Logurilor:** Fluxurile standard `stdout` și `stderr` ale aplicațiilor copil sunt decuplate de consolă și redirecționate în fișiere log separate (`app_output.log`, `refresh_output.log`), permițând diagnosticarea erorilor asincrone fără a bloca firele principale de execuție.

#### B. Modulul de Achiziție Date și Grafică Blitting (`caen_monitor_v6_optimized.py`)

Este nucleul analitic al aplicației, optimizat pentru amprentă redusă de memorie.

* **Randare Grafică Ultrarapidă prin Blitting (Matplotlib):** Într-o randare grafică standard (F5 la fiecare cadru), Matplotlib redesenează axele, gridul, textul și etichetele, generând un consum masiv de CPU (care după câteva zile blochează aplicația). Acest modul implementează **Blitting**: la pornire se capturează fundalul static într-un buffer de pixeli (`copy_from_bbox`). La fiecare ciclu de 2 secunde, fundalul este restaurat instantaneu (`restore_region`), și sunt redesenate exclusiv noile puncte ale liniilor de date (`draw_artist` + `blit`), reducând consumul CPU cu peste 90%.
* **Colectare Optimizată prin Pre-Serializare și Refolosire de Buffer:** Pentru a minimiza intervenția Garbage Collector-ului (GC) din Python (care poate cauza micro-înghețări ale aplicației), comenzile JSON destinate aparatului CAEN sunt pre-serializate o singură dată la inițializare. De asemenea, listele și dicționarele în care se descarcă datele sunt alocate static în memorie și suprascrise (nu recreate) la fiecare ciclu de 10 secunde.
* **Sistem Dual de ntfy (Worker persistent bazat pe Coadă):** Notificările push către smartphone sunt gestionate printr-un fir de execuție dedicat (`threading.Thread`) alimentat de o coadă FIFO (`queue.Queue`). Acest lucru elimină overhead-ul creării unui thread nou la fiecare alertă trimisă și previne blocarea buclei principale în cazul în care serverul ntfy are latențe mari.

#### C. Modulul Selenium de Înaltă Stabilitate (`refresh_page1.py`)

Reconstruit integral pentru a asigura o interacțiune deterministică (fără erori aleatoare) cu serverul web al aparatului.

* **Watchdog Intern pentru Selenium:** Uneori, driverul Chrome (sau conexiunea de rețea) agață execuția unei comenzi Selenium la nivel de socket-uri native, determinând scriptul să aștepte la infinit. Watchdog-ul asincron monitorizează variabila `last_activity`. Dacă aceasta depășește pragul stabilit, thread-ul paralel forțează închiderea socket-ului driverului din exterior, deblocând imediat bucla principală.
* **Login 100% Fiabil cu Algoritm de Feedback Loops:** În loc să trimită caracterele parolei orbește, scriptul scrie parola, iar apoi interoghează imediat DOM-ul paginii (`get_attribute("value")`). Dacă valoarea nu corespunde din cauza unei latențe în browser, câmpul este curățat și rescris. Trimiterea formularului (`click` sau `RETURN`) este condiționată strict de această confirmare.
* **Așteptare Inteligentă (Event-Driven / Non-Blocking):** Toate apelurile de tip `time.sleep()` din timpul navigării au fost eliminate. Încărcarea paginii se validează prin interogarea motorului Javascript (`document.readyState == "complete"`), iar tranziția elementelor se face prin `WebDriverWait` (bazat pe polling adaptiv la nivel de DOM).
* **Detectarea Automată a Sesiunii (State Awareness):** Scriptul verifică prezența selectorului de login sau a indicatorului de utilizator logat înainte de a acționa. Dacă sesiunea (cookie/local storage) din profilul dedicat persistent (`chrome_refresh_profile`) este validă, procesul de autentificare este omis, conservând resursele aparatului.

---

### 3. Mecanisme Avansate de Stabilitate implementate

1. **Scriere Atomică a Configurațiilor:** Când aplicația salvează starea (ex: ultimul path, statusul de reboot), nu scrie direct în `config.json`. Creează un fișier temporar `config.json.tmp`, scrie datele, apoi rulează metoda nativă de sistem `os.replace()`. Aceasta asigură o operațiune atomică la nivel de OS — dacă sistemul se oprește brusc în timpul scrierii, fișierul de configurare **nu se va corupe niciodată**.
2. **Prevenirea Instanțelor Multiple (Port-Based Mutex):** Pentru a împiedica pornirea accidentală a două instanțe ale aplicației (ceea ce ar duce la coruperea fișierului CSV de date și la coliziuni pe portul WebSocket), aplicația principală deschide și blochează un socket local pe portul `51423`. Dacă o a doua instanță încearcă să pornească, aceasta va primi o eroare de tipul "Port deja utilizat" și se va închide politicos, afișând un mesaj de avertizare.
3. **Loguri Rotative Automate:** Toate fișierele de log (atât cele de mesaje, cât și cele de erori) au o limită superioară fixă (5 MB). La atingerea acesteia, fișierul este redenumit în `.1` (suprascriind arhiva veche) și un fișier nou este generat. Spațiul pe hard disk este astfel plafonat matematic, eliminând riscul blocării sistemului de operare din cauza lipsei de spațiu.

### 4. Stack-ul Tehnologic (Tech Stack)

* **Limbaj principal:** Python 3.x
* **Interfață Grafică:** CustomTkinter (UI Modern asincron bazat pe Tkinter orientat pe obiecte).
* **Automatizare Web:** Selenium WebDriver + Google Chrome Core.
* **Protocol Comunicare Date:** `websocket-client` (Conexiuni TCP persistente, duplex de mare viteză).
* **Randare Grafică:** Matplotlib (configurat în mod nativ pentru Blitting/TkAgg Canvas Integration).
* **Monitorizare OS:** `psutil` (Inspecție procese la nivel de Kernel).
* **Protocol Alerte:** REST API (HTTPS POST) către serviciul securizat `ntfy.sh`.

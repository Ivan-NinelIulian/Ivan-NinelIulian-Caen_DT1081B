import websocket
import json
import time
import threading
import queue
import os
import traceback
import requests
import customtkinter as ctk
from tkinter import filedialog, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from datetime import datetime
from collections import deque
import gc
import socket

# --- CONFIGURARE ---
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")


def _load_device_config():
    """Citeste adresa aparatului dintr-un fisier de config comun cu
    supervisor_v2.py si refresh_page1.py (device_config.json), ca adresa sa
    se seteze o singura data, intr-un singur loc, dupa ce esti conectat la
    aparat prin Ethernet - in loc sa fie hardcodata separat in fiecare script."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "device_config.json")
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                ip = data.get("ip_aparat")
                if ip:
                    return ip
    except Exception:
        pass
    return "localhost"


IP_APARAT = _load_device_config()
WS_URL = f"ws://{IP_APARAT}:8080/"
HTTP_BASE = f"http://{IP_APARAT}"
NTFY_TOPIC = "monitor_caen_ninii_2026"

WS_CONNECT_TIMEOUT = 8
WS_RECV_TIMEOUT    = 6
CYCLE_INTERVAL     = 10.0
RETRY_SLEEP        = 5
MAX_CONSECUTIVE_ERRORS = 3

# --- Reconectare cu backoff exponential ---
CONNECT_BACKOFF_BASE = 5     # secunde
CONNECT_BACKOFF_MAX  = 60    # plafon, ca sa nu asteptam prea mult intre incercari

# --- Watchdog / detectie blocaj ---
WATCHDOG_CHECK_INTERVAL      = 15   # cat de des verifica watchdog-ul sanatatea sistemului
HEARTBEAT_STALE_THRESHOLD    = 90   # secunde fara date reale => suspect blocat
STALE_DATA_REPEAT_THRESHOLD  = 30   # cicluri consecutive cu date IDENTICE => aparatul raspunde dar e "inghetat"

# --- Instanta unica (evita 2 copii ale aplicatiei rulate simultan) ---
SINGLE_INSTANCE_PORT = 51423

# --- Intretinere pe termen lung (log-uri, garbage collection) ---
CRASH_LOG_MAX_BYTES = 5 * 1024 * 1024   # 5 MB, dupa care rotam crash_log.txt
GC_COLLECT_INTERVAL_CYCLES = 180        # forteaza gc.collect() o data la ~180 cicluri (~30 min), nu la fiecare ciclu

# Comenzi WS de reboot testate in ordine
WS_REBOOT_CANDIDATES = [
    {"command": "reboot",        "params": {},                "SessionKey": ""},
    {"command": "restart",       "params": {},                "SessionKey": ""},
    {"command": "system_reboot", "params": {},                "SessionKey": ""},
    {"command": "set_command",   "params": {"cmd": "reboot"}, "SessionKey": ""},
]

# Endpoint-uri HTTP de reboot testate in ordine
HTTP_REBOOT_CANDIDATES = [
    ("POST", "/api/reboot"),
    ("POST", "/cgi-bin/reboot.cgi"),
    ("GET",  "/reboot"),
    ("POST", "/reboot"),
    ("GET",  "/api/restart"),
    ("POST", "/api/restart"),
    ("GET",  "/cgi-bin/restart.cgi"),
]

HTTP_REBOOT_TIMEOUT = 5

# --- SOFT REFRESH (simuleaza F5 pe pagina web, fara reboot fizic) ---
SOFT_REFRESH_CANDIDATES = [
    ("GET", "/"),
    ("GET", "/index.html"),
    ("GET", "/api/status"),
    ("GET", "/api/init"),
    ("GET", "/api/session"),
    ("GET", "/cgi-bin/status.cgi"),
]
SOFT_REFRESH_TIMEOUT = 5
SOFT_REFRESH_ERROR_THRESHOLD = 2


class SafeWebSocket:
    def __init__(self, url, connect_timeout=WS_CONNECT_TIMEOUT, recv_timeout=WS_RECV_TIMEOUT):
        self.url = url
        self.connect_timeout = connect_timeout
        self.recv_timeout = recv_timeout
        self._ws = None
        self._lock = threading.Lock()

    def connect(self):
        self.close()
        ws = websocket.create_connection(
            self.url, timeout=self.connect_timeout,
            header={"User-Agent": "Mozilla/5.0"},
        )
        ws.sock.settimeout(self.recv_timeout)
        self._ws = ws

    def send(self, data):
        with self._lock:
            if self._ws is None:
                raise ConnectionError("WebSocket nu este conectat")
            self._ws.send(data)

    def recv(self):
        with self._lock:
            if self._ws is None:
                raise ConnectionError("WebSocket nu este conectat")
            return self._ws.recv()

    def close(self):
        with self._lock:
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None

    @property
    def connected(self):
        return self._ws is not None


class CAENProMonitor(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("CAEN Monitor v6")
        self.geometry("1600x950")
        self.configure(fg_color="#f5f6fa")

        # Prinde erorile aparute in callback-urile Tkinter (ex: click-uri, redesenari)
        # ca sa nu crape toata aplicatia silentios.
        self.report_callback_exception = self._handle_tk_exception

        self._ws = SafeWebSocket(WS_URL)
        # Sesiune HTTP unica, refolosita pentru toate cererile (ntfy, soft-refresh,
        # reboot). O noua conexiune TCP+TLS la fiecare cerere e risipa de timp/CPU
        # cand aplicatia ruleaza zile la rand si trimite sute/mii de cereri -
        # requests.Session() tine conexiunile deschise (keep-alive) si le refoloseste.
        self._http = requests.Session()
        self.running = False
        self._worker_thread = None
        self._watchdog_thread = None
        self.erori_consecutive = 0
        self.consecutive_connect_failures = 0
        self._cycle_count = 0  # folosit pentru gc.collect() rar, nu la fiecare ciclu

        # Coada de notificari ntfy + UN singur fir de fundal persistent care le
        # trimite in ordine. Inainte, fiecare send_ntfy() crea un thread OS nou
        # (creare/distrugere thread costa timp/memorie) - pe o aplicatie care
        # ruleaza saptamani si trimite sute de notificari, un singur worker
        # persistent e mult mai eficient si evita acumularea de thread-uri.
        self._ntfy_queue = queue.Queue()
        self._ntfy_thread = threading.Thread(target=self._ntfy_worker, daemon=True, name="CAEN-Ntfy")
        self._ntfy_thread.start()

        # Comenzile WS de interogare a sectiunilor sunt fixe (doar section id
        # variaza) - le pre-serializam o singura data in loc sa reconstruim
        # dict-ul + json.dumps() la fiecare sectiune, la fiecare ciclu de 10s.
        self._query_json = {
            s_id: json.dumps({'command': 'get_function_results', 'callback': 'tot',
                               'params': {'section': s_id}, 'SessionKey': ''})
            for s_id in range(4)
        }

        # Buffer reutilizabil pentru ciclul de achizitie, ca sa nu alocam un
        # dict + 4 liste noi la fiecare ciclu de 10s (mai putina presiune pe GC
        # pe rulari de zile/saptamani).
        self._temp_cycle_buffer = {i: [0.0, 0.0, 0.0, 0.0] for i in range(4)}

        # Stare pentru randare grafic cu blitting (vezi refresh_ui_loop).
        self._plot_backgrounds = {}
        self._plot_backgrounds_ready = False
        self._plot_redraw_counter = 0

        self.nume_sectiuni = {0: "A", 1: "B", 2: "C", 3: "D"}
        self.buffer_date = {i: [0.0] * 4 for i in range(4)}
        self.max_points = 50

        self.plot_data = {
            s: {str(i): deque([0.0] * self.max_points, maxlen=self.max_points)
                for i in range(1, 5)}
            for s in ["A", "B", "C", "D"]
        }

        # Config incarcata o singura data si tinuta in memorie (self._config_cache).
        # save_config() nu mai re-citeste fisierul de pe disc la fiecare apel -
        # doar actualizeaza cache-ul si scrie atomic. Instanta unica (single
        # instance lock) garanteaza ca nimeni altcineva nu modifica fisierul
        # intre timp, deci cache-ul ramane mereu corect.
        self._config_cache = self.load_config()
        config = self._config_cache
        self.target_folder       = config.get("last_path", os.getcwd())
        self.saved_password      = config.get("pwd", "")
        self.interval_ore        = config.get("interval_ore", 24)
        self.last_file_time      = config.get("last_file_time", 0)
        self.current_active_file = config.get("last_file_name", "")
        self.reboot_enabled      = config.get("reboot_enabled", False)
        self.last_reboot_time    = config.get("last_reboot_time", 0)
        self._reboot_method_ws   = config.get("reboot_method_ws", None)
        self._reboot_method_http = config.get("reboot_method_http", None)

        # --- Soft refresh state ---
        self._refresh_method_http      = config.get("refresh_method_http", None)
        self.refresh_enabled           = config.get("refresh_enabled", True)
        self.refresh_interval_ore      = config.get("refresh_interval_ore", 6)
        self.last_refresh_time         = config.get("last_refresh_time", 0)
        self._soft_refresh_in_progress = False

        # --- Login / pornire neasistata ---
        self.login_configured = config.get("login_configured", False)

        # --- Watchdog / stabilitate state ---
        self.auto_start_enabled   = config.get("auto_start_enabled", False)
        self.last_heartbeat       = time.time()
        self._last_data_snapshot  = None
        self._same_data_count     = 0
        self._last_watchdog_soft_refresh = 0
        self._last_watchdog_restart      = 0
        self.worker_restart_count = config.get("worker_restart_count", 0)

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.show_login_screen()

    # --- Config ---
    def load_config(self):
        try:
            if os.path.exists("config.json"):
                with open("config.json", "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def save_config(self, **kwargs):
        # Actualizam cache-ul din memorie in loc sa recitim fisierul de pe disc
        # la fiecare salvare (evita un read+json.parse inutil de fiecare data
        # cand se schimba o singura setare - conteaza pe rulari lungi cu multe
        # salvari: restart-uri worker, detectie metoda reboot/refresh etc).
        self._config_cache.update(kwargs)
        try:
            tmp_path = "config.json.tmp"
            with open(tmp_path, "w") as f:
                json.dump(self._config_cache, f)
            os.replace(tmp_path, "config.json")  # scriere atomica, evita config corupt daca se opreste brusc
        except Exception:
            pass

    # --- ntfy ---
    def send_ntfy(self, message, priority="default", tags=""):
        # Doar punem mesajul in coada - firul dedicat _ntfy_worker il trimite.
        # Nu blocheaza niciodata apelantul si nu mai creeaza un thread nou de
        # fiecare data.
        self._ntfy_queue.put((message, priority, tags))

    def _ntfy_worker(self):
        while True:
            message, priority, tags = self._ntfy_queue.get()
            try:
                self._http.post(
                    f"https://ntfy.sh/{NTFY_TOPIC}",
                    data=message.encode("utf-8"),
                    headers={"Title": "Sistem CAEN", "Priority": priority, "Tags": tags},
                    timeout=5,
                )
            except Exception:
                pass

    # --- Crash log (pentru diagnosticare, nu pentru activitate normala) ---
    # Rotit automat cand depaseste CRASH_LOG_MAX_BYTES, ca sa nu creasca
    # la infinit dupa luni/ani de rulare continua.
    def _rotate_crash_log_if_needed(self):
        try:
            if os.path.exists("crash_log.txt") and os.path.getsize("crash_log.txt") > CRASH_LOG_MAX_BYTES:
                backup = "crash_log.txt.1"
                if os.path.exists(backup):
                    os.remove(backup)
                os.replace("crash_log.txt", backup)
        except Exception:
            pass

    def _append_crash_log(self, text):
        try:
            self._rotate_crash_log_if_needed()
            with open("crash_log.txt", "a", encoding="utf-8") as f:
                f.write(f"\n--- {datetime.now().isoformat()} ---\n{text}\n")
        except Exception:
            pass

    def _handle_tk_exception(self, exc, val, tb):
        err_text = "".join(traceback.format_exception(exc, val, tb))
        self._append_crash_log(f"Tkinter callback exception:\n{err_text}")
        try:
            self.log_msg(f"Eroare UI neasteptata (vezi crash_log.txt): {val}")
        except Exception:
            pass

    # --- SOFT REFRESH (simulare F5) ---
    def soft_refresh_device(self):
        if self._soft_refresh_in_progress:
            self.log_msg("Soft-refresh deja in curs, ignor cererea noua.")
            return
        self.log_msg(">>> Soft-refresh (simulare F5 pagina web)...")
        threading.Thread(target=self._do_soft_refresh, daemon=True, name="CAEN-SoftRefresh").start()

    def _do_soft_refresh(self):
        self._soft_refresh_in_progress = True
        try:
            success = False
            if self._refresh_method_http is not None:
                method, endpoint = SOFT_REFRESH_CANDIDATES[self._refresh_method_http]
                success = self._try_http_refresh(method, endpoint, self._refresh_method_http, save=False)
            else:
                for idx, (method, endpoint) in enumerate(SOFT_REFRESH_CANDIDATES):
                    if self._try_http_refresh(method, endpoint, idx, save=True):
                        success = True
                        break

            self._ws.close()

            self.last_refresh_time = time.time()
            self.save_config(last_refresh_time=self.last_refresh_time)

            if success:
                self.log_msg("Soft-refresh trimis cu succes.")
            else:
                self.log_msg("Soft-refresh: niciun endpoint HTTP nu a raspuns, am reinitiat doar WS.")
        finally:
            self._soft_refresh_in_progress = False

    def _try_http_refresh(self, method, endpoint, idx, save):
        url = f'{HTTP_BASE}{endpoint}'
        try:
            resp = self._http.request(
                method, url,
                timeout=SOFT_REFRESH_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                },
                auth=(self.saved_password, self.saved_password) if self.saved_password else None,
            )
            self.log_msg(f'  Soft-refresh {method} {endpoint} -> {resp.status_code}')
            if resp.status_code < 400:
                if save:
                    self._refresh_method_http = idx
                    self.save_config(refresh_method_http=idx)
                return True
        except Exception as e:
            self.log_msg(f'  Soft-refresh {method} {endpoint} esuat: {type(e).__name__}')
        return False

    # --- REBOOT ---
    def reboot_device(self):
        self.log_msg(">>> Initiere reboot CAEN...")
        self.send_ntfy("Reboot device CAEN initiat", "default", "arrows_counterclockwise")
        threading.Thread(target=self._do_reboot, daemon=True, name="CAEN-Reboot").start()

    def _do_reboot(self):
        success = False
        if self._reboot_method_ws is not None:
            cmd = WS_REBOOT_CANDIDATES[self._reboot_method_ws]
            success = self._try_ws_reboot(cmd, self._reboot_method_ws, save=False)
        else:
            for idx, cmd in enumerate(WS_REBOOT_CANDIDATES):
                if self._try_ws_reboot(cmd, idx, save=True):
                    success = True
                    break
        if success:
            self.log_msg("Reboot WS trimis. Astept repornire...")
            self._wait_for_device_back()
            return
        if self._reboot_method_http is not None:
            method, endpoint = HTTP_REBOOT_CANDIDATES[self._reboot_method_http]
            success = self._try_http_reboot(method, endpoint, self._reboot_method_http, save=False)
        else:
            for idx, (method, endpoint) in enumerate(HTTP_REBOOT_CANDIDATES):
                if self._try_http_reboot(method, endpoint, idx, save=True):
                    success = True
                    break
        if success:
            self.log_msg("Reboot HTTP trimis. Astept repornire...")
            self._wait_for_device_back()
        else:
            self.log_msg("EROARE Reboot ESUAT - nicio metoda nu a functionat!")
            self.send_ntfy("Reboot CAEN ESUAT - interventie manuala necesara!", "urgent", "rotating_light")

    def _try_ws_reboot(self, cmd, idx, save):
        try:
            ws_temp = websocket.create_connection(
                WS_URL, timeout=WS_CONNECT_TIMEOUT,
                header={"User-Agent": "Mozilla/5.0"},
            )
            ws_temp.sock.settimeout(WS_RECV_TIMEOUT)
            ws_temp.send(json.dumps(cmd))
            time.sleep(0.5)
            try:
                resp = ws_temp.recv()
                self.log_msg(f'  WS reboot [{idx}] raspuns: {resp[:80]}')
                resp_data = json.loads(resp)
                if resp_data.get("status") not in ("error", "unknown_command", "invalid"):
                    if save:
                        self._reboot_method_ws = idx
                        self.save_config(reboot_method_ws=idx)
                    ws_temp.close()
                    return True
            except Exception:
                self.log_msg(f'  WS reboot [{idx}] - device a inchis conexiunea (semn bun)')
                if save:
                    self._reboot_method_ws = idx
                    self.save_config(reboot_method_ws=idx)
                return True
            ws_temp.close()
        except Exception as e:
            self.log_msg(f'  WS reboot [{idx}] esuat: {type(e).__name__}')
        return False

    def _try_http_reboot(self, method, endpoint, idx, save):
        url = f'{HTTP_BASE}{endpoint}'
        try:
            resp = self._http.request(
                method, url,
                timeout=HTTP_REBOOT_TIMEOUT,
                auth=(self.saved_password, self.saved_password) if self.saved_password else None,
            )
            self.log_msg(f'  HTTP reboot {method} {endpoint} -> {resp.status_code}')
            if resp.status_code < 400:
                if save:
                    self._reboot_method_http = idx
                    self.save_config(reboot_method_http=idx)
                return True
        except requests.exceptions.ConnectionError:
            self.log_msg(f'  HTTP reboot {method} {endpoint} -> conexiune refuzata (semn bun)')
            if save:
                self._reboot_method_http = idx
                self.save_config(reboot_method_http=idx)
            return True
        except Exception as e:
            self.log_msg(f'  HTTP reboot {method} {endpoint} esuat: {type(e).__name__}')
        return False

    def _wait_for_device_back(self):
        self._ws.close()
        self._update_status(False)
        deadline = time.time() + 180
        while time.time() < deadline:
            time.sleep(5)
            try:
                s = socket.create_connection((IP_APARAT, 8080), timeout=3)
                s.close()
                self.log_msg("Device CAEN este din nou online!")
                self.send_ntfy("Device CAEN repornit cu succes!", "default", "white_check_mark")
                self.last_reboot_time = time.time()
                self.save_config(last_reboot_time=self.last_reboot_time)
                return
            except Exception:
                self.log_msg("Astept device sa revina online...")
        self.log_msg("ATENTIE Device nu a revenit in 3 minute!")
        self.send_ntfy("Device CAEN nu a revenit dupa reboot!", "high", "warning")

    # --- Login ---
    def show_login_screen(self):
        if self.login_configured:
            self._start_main_app()
            return
        self.login_win = ctk.CTkToplevel(self)
        self.login_win.title("Login CAEN")
        self.login_win.geometry("400x300")
        self.login_win.grab_set()
        ctk.CTkLabel(self.login_win, text="AUTENTIFICARE", font=("Segoe UI", 20, "bold")).pack(pady=20)
        self.ent_pwd = ctk.CTkEntry(self.login_win, width=250, show="*", placeholder_text="Parola...")
        self.ent_pwd.insert(0, self.saved_password)
        self.ent_pwd.pack(pady=10)
        ctk.CTkButton(self.login_win, text="CONECTARE", command=self.verify_login).pack(pady=20)
        ctk.CTkLabel(self.login_win, text="(o singura data - data viitoare porneste automat)",
                     font=("Segoe UI", 9), text_color="#7f8c8d").pack()

    def verify_login(self):
        self.saved_password = self.ent_pwd.get()
        self.login_configured = True
        self.save_config(pwd=self.saved_password, login_configured=True)
        self.login_win.destroy()
        self._start_main_app()

    def _start_main_app(self):
        self.setup_ui()
        self.check_and_manage_file()
        self.refresh_ui_loop()
        self.start_watchdog()
        self.start_refresh_scheduler()
        if self.auto_start_enabled:
            self.log_msg("Pornire automata achizitie (setare activata).")
            self.start_acq()

    # --- Scheduler soft-refresh (independent de achizitie) ---
    # Fir separat, dedicat, care garanteaza: (1) un soft-refresh chiar la
    # pornirea aplicatiei, indiferent daca achizitia e pornita sau nu, si
    # (2) soft-refresh automat la fiecare `refresh_interval_ore`, cat timp
    # aplicatia ruleaza - nu doar cat timp ruleaza achizitia.
    def start_refresh_scheduler(self):
        threading.Thread(target=self._refresh_scheduler_loop, daemon=True,
                          name="CAEN-RefreshScheduler").start()

    def _refresh_scheduler_loop(self):
        if self.refresh_enabled:
            self.log_msg("Refresh initial la pornirea aplicatiei...")
            self.soft_refresh_device()
        else:
            self.log_msg("Soft-refresh dezactivat - nu fac refresh la pornire.")

        while True:
            time.sleep(5)
            if not self.refresh_enabled:
                continue
            limita = self.refresh_interval_ore * 3600
            if time.time() - self.last_refresh_time >= limita:
                self.log_msg(f"Interval de {self.refresh_interval_ore}h atins -> refresh automat pagina.")
                self.soft_refresh_device()

    def cmd_change_password(self):
        win = ctk.CTkToplevel(self)
        win.title("Schimba parola")
        win.geometry("380x220")
        win.grab_set()
        ctk.CTkLabel(win, text="PAROLA NOUA", font=("Segoe UI", 16, "bold")).pack(pady=15)
        ent = ctk.CTkEntry(win, width=230, show="*", placeholder_text="Parola...")
        ent.insert(0, self.saved_password)
        ent.pack(pady=10)

        def salveaza():
            self.saved_password = ent.get()
            self.save_config(pwd=self.saved_password)
            self.log_msg("Parola a fost actualizata.")
            win.destroy()

        ctk.CTkButton(win, text="SALVEAZA", command=salveaza).pack(pady=15)

    # --- Fisiere ---
    def check_and_manage_file(self):
        acum = time.time()
        limita_secunde = self.interval_ore * 3600
        fisier_existent = os.path.exists(os.path.join(self.target_folder, self.current_active_file))
        if self.current_active_file and fisier_existent and (acum - self.last_file_time < limita_secunde):
            self.log_msg(f'Continuare fisier existent: {self.current_active_file}')
            self.lbl_active_file.configure(text=f'Activ: {self.current_active_file}')
        else:
            self.cmd_create_new_file_only()

    def cmd_create_new_file_only(self):
        self.last_file_time = time.time()
        self.current_active_file = f"DATA_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path = os.path.join(self.target_folder, self.current_active_file)
        try:
            with open(path, "w") as f:
                f.write("Timestamp;A1;A2;A3;A4;B1;B2;B3;B4;C1;C2;C3;C4;D1;D2;D3;D4\n")
        except Exception as e:
            # Daca folderul de salvare devine inaccesibil (disc detasat, retea
            # picata etc.) nu vrem sa crape toata aplicatia - doar semnalam.
            self.log_msg(f'EROARE la crearea fisierului nou: {e}')
            self.send_ntfy(f'Nu pot crea fisier nou de date: {e}', 'urgent', 'rotating_light')
            return
        self.lbl_active_file.configure(text=f'Activ: {self.current_active_file}')
        self.save_config(last_file_time=self.last_file_time, last_file_name=self.current_active_file)
        self.log_msg(f'Fisier nou creat: {self.current_active_file}')
        self.send_ntfy(f'Fisier nou: {self.current_active_file}', 'low', 'file_folder')
        if self.reboot_enabled:
            self.reboot_device()

    # --- UI ---
    def setup_ui(self):
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.sidebar = ctk.CTkFrame(self, width=280, corner_radius=0,
                                    fg_color="#ffffff", border_width=1, border_color="#dcdde1")
        self.sidebar.grid(row=0, column=0, sticky="nsew")

        ctk.CTkLabel(self.sidebar, text="CONTROL", font=("Segoe UI", 18, "bold")).pack(pady=10)

        ctk.CTkLabel(self.sidebar, text="Interval fisier nou (ore):", font=("Segoe UI", 11)).pack(pady=(10, 0))
        self.opt_interval = ctk.CTkOptionMenu(self.sidebar, values=["1", "6", "12", "24", "48", "168"],
                                              command=self.change_interval)
        self.opt_interval.set(str(self.interval_ore))
        self.opt_interval.pack(padx=20, pady=5, fill="x")

        self.sw_auto_start = ctk.CTkSwitch(self.sidebar, text='Pornire automata la lansare',
                                           command=self.toggle_auto_start, font=('Segoe UI', 11))
        if self.auto_start_enabled:
            self.sw_auto_start.select()
        self.sw_auto_start.pack(padx=20, pady=(5, 10), anchor="w")

        # --- WATCHDOG / STARE PANEL ---
        watchdog_frame = ctk.CTkFrame(self.sidebar, fg_color='#eafaf1', corner_radius=8,
                                      border_width=1, border_color='#2ecc71')
        watchdog_frame.pack(padx=15, pady=(0, 5), fill="x")
        ctk.CTkLabel(watchdog_frame, text='Watchdog',
                     font=('Segoe UI', 11, 'bold'), text_color='#27ae60').pack(pady=(6, 0))
        self.lbl_heartbeat = ctk.CTkLabel(watchdog_frame, text='Ultima transmisie: -',
                                          font=('Segoe UI', 10))
        self.lbl_heartbeat.pack(pady=(2, 0))
        self.lbl_restarts = ctk.CTkLabel(watchdog_frame, text='Restart-uri worker: 0',
                                         font=('Segoe UI', 10))
        self.lbl_restarts.pack(pady=(0, 6))
        # --- END WATCHDOG PANEL ---

        # --- SOFT REFRESH PANEL ---
        refresh_frame = ctk.CTkFrame(self.sidebar, fg_color='#e8f6ff', corner_radius=8,
                                     border_width=1, border_color='#3498db')
        refresh_frame.pack(padx=15, pady=(5, 5), fill="x")
        ctk.CTkLabel(refresh_frame, text='Soft-Refresh (simuleaza F5)',
                     font=('Segoe UI', 11, 'bold'), text_color='#2980b9').pack(pady=(6, 0))
        ctk.CTkLabel(refresh_frame, text='Preventiv + automat la erori/blocaj',
                     font=('Segoe UI', 9), text_color='#7f8c8d').pack()
        self.sw_refresh = ctk.CTkSwitch(refresh_frame, text='Activat',
                                        command=self.toggle_refresh, font=('Segoe UI', 11))
        if self.refresh_enabled:
            self.sw_refresh.select()
        self.sw_refresh.pack(pady=4)
        self.opt_refresh_interval = ctk.CTkOptionMenu(refresh_frame, values=["1", "2", "3", "6", "12", "24"],
                                                       command=self.change_refresh_interval)
        self.opt_refresh_interval.set(str(self.refresh_interval_ore))
        self.opt_refresh_interval.pack(padx=10, pady=(0, 6), fill="x")
        ctk.CTkButton(refresh_frame, text='REFRESH ACUM',
                      fg_color='#3498db', hover_color='#2980b9',
                      font=('Segoe UI', 11, 'bold'),
                      command=self.soft_refresh_device).pack(padx=10, pady=(0, 8), fill="x")
        # --- END SOFT REFRESH PANEL ---

        # --- REBOOT PANEL ---
        reboot_frame = ctk.CTkFrame(self.sidebar, fg_color='#fff8e1', corner_radius=8,
                                    border_width=1, border_color='#f39c12')
        reboot_frame.pack(padx=15, pady=(5, 5), fill="x")
        ctk.CTkLabel(reboot_frame, text='Reboot automat device',
                     font=('Segoe UI', 11, 'bold'), text_color='#e67e22').pack(pady=(6, 0))
        ctk.CTkLabel(reboot_frame, text='La interval fisier + dupa blocaj prelungit',
                     font=('Segoe UI', 9), text_color='#7f8c8d').pack()
        self.sw_reboot = ctk.CTkSwitch(reboot_frame, text='Activat',
                                       command=self.toggle_reboot, font=('Segoe UI', 11))
        if self.reboot_enabled:
            self.sw_reboot.select()
        self.sw_reboot.pack(pady=6)
        ctk.CTkButton(reboot_frame, text='REBOOT MANUAL',
                      fg_color='#e67e22', hover_color='#d35400',
                      font=('Segoe UI', 11, 'bold'),
                      command=self.reboot_device).pack(padx=10, pady=(0, 8), fill="x")
        # --- END REBOOT PANEL ---

        self.btn_start = ctk.CTkButton(self.sidebar, text="START ACHIZITIE",
                                       fg_color="#2ecc71", command=self.start_acq)
        self.btn_start.pack(padx=20, pady=5, fill="x")
        self.btn_stop = ctk.CTkButton(self.sidebar, text="STOP",
                                      fg_color="#e74c3c", command=self.stop_acq, state="disabled")
        self.btn_stop.pack(padx=20, pady=5, fill="x")

        ctk.CTkButton(self.sidebar, text="FOLDER SALVARE",
                      fg_color="#34495e", command=self.cmd_choose_folder).pack(padx=20, pady=10, fill="x")

        ctk.CTkButton(self.sidebar, text="SCHIMBA PAROLA",
                      fg_color="#95a5a6", hover_color="#7f8c8d",
                      command=self.cmd_change_password).pack(padx=20, pady=(0, 5), fill="x")

        self.lbl_active_file = ctk.CTkLabel(self.sidebar, text="Activ: -", font=("Segoe UI", 10, "italic"))
        ctk.CTkButton(self.sidebar, text="FISIER NOU FORTAT",
                      fg_color="#f39c12", command=self.cmd_create_new_file_only).pack(padx=20, pady=5, fill="x")
        self.lbl_active_file.pack(padx=10)

        self.lbl_status = ctk.CTkLabel(self.sidebar, text="Deconectat",
                                       font=("Segoe UI", 11, "bold"), text_color="#e74c3c")
        self.lbl_status.pack(pady=(10, 0))

        self.txt_log = ctk.CTkTextbox(self.sidebar, height=200, font=("Consolas", 10))
        self.txt_log.pack(padx=15, pady=15, fill="x")

        self.main_panel = ctk.CTkFrame(self, fg_color='transparent')
        self.main_panel.grid(row=0, column=1, sticky="nsew", padx=20, pady=20)

        self.top_monitor = ctk.CTkFrame(self.main_panel, fg_color='transparent')
        self.top_monitor.pack(fill='x', pady=(0, 20))
        self.ui_labels = {}
        for litera in ["A", "B", "C", "D"]:
            card = ctk.CTkFrame(self.top_monitor, fg_color='#ffffff', corner_radius=12,
                                border_width=1, border_color='#dcdde1')
            card.pack(side='left', expand=True, fill='both', padx=5)
            ctk.CTkLabel(card, text=f'Sectiunea {litera}',
                         font=('Segoe UI', 12, 'bold'), text_color='#3498db').pack(pady=5)
            self.ui_labels[litera] = []
            for _ in range(4):
                lbl = ctk.CTkLabel(card, text='0.00', font=('Consolas', 16, 'bold'))
                lbl.pack()
                self.ui_labels[litera].append(lbl)

        self.fig, self.axes_array = plt.subplots(2, 2, figsize=(10, 6))
        self.axes = {'A': self.axes_array[0, 0], 'B': self.axes_array[0, 1],
                     'C': self.axes_array[1, 0], 'D': self.axes_array[1, 1]}
        # Linii persistente, create o singura data. La fiecare actualizare doar
        # le mutam datele (set_ydata) in loc sa stergem si redesenam totul
        # (ax.clear() + ax.plot() + titlu + etc de zeci de mii de ori pe zi) -
        # mult mai putin CPU/memorie pe rulari lungi (zile/saptamani).
        self.plot_lines = {}
        x_vals = list(range(self.max_points))
        for char, ax in self.axes.items():
            ax.set_title('Flux µ m²', fontsize=10, fontweight='bold')
            self.plot_lines[char] = []
            for i in range(1, 5):
                (line,) = ax.plot(x_vals, list(self.plot_data[char][str(i)]))
                # animated=True scoate liniile din randarea normala a figurii,
                # ca sa poata fi desenate separat, rapid, prin blitting (vezi
                # refresh_ui_loop) - reduce mult CPU-ul folosit de matplotlib
                # pe actualizari repetate, la infinit, cat timp ruleaza aplicatia.
                line.set_animated(True)
                self.plot_lines[char].append(line)
            ax.relim()
            ax.autoscale_view()
        plt.tight_layout(pad=3.0)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.main_panel)
        self.canvas.get_tk_widget().pack(fill='both', expand=True)

        # La redimensionare fereastra, fundalul cache-uit pentru blitting nu
        # mai e valid - il invalidam, ceea ce forteaza un redraw complet la
        # urmatoarea actualizare (vezi refresh_ui_loop).
        self.canvas.mpl_connect('resize_event', lambda evt: setattr(self, '_plot_backgrounds_ready', False))

        # Primul desen complet + captura fundalului pentru blitting.
        self.canvas.draw()
        self._plot_backgrounds = {char: self.canvas.copy_from_bbox(ax.bbox) for char, ax in self.axes.items()}
        self._plot_backgrounds_ready = True

    def toggle_reboot(self):
        self.reboot_enabled = bool(self.sw_reboot.get())
        self.save_config(reboot_enabled=self.reboot_enabled)
        status = 'ACTIVAT' if self.reboot_enabled else 'DEZACTIVAT'
        self.log_msg(f'Reboot automat {status} (interval: {self.interval_ore}h)')

    def toggle_refresh(self):
        self.refresh_enabled = bool(self.sw_refresh.get())
        self.save_config(refresh_enabled=self.refresh_enabled)
        status = 'ACTIVAT' if self.refresh_enabled else 'DEZACTIVAT'
        self.log_msg(f'Soft-refresh automat {status} (interval: {self.refresh_interval_ore}h)')

    def toggle_auto_start(self):
        self.auto_start_enabled = bool(self.sw_auto_start.get())
        self.save_config(auto_start_enabled=self.auto_start_enabled)
        status = 'ACTIVATA' if self.auto_start_enabled else 'DEZACTIVATA'
        self.log_msg(f'Pornire automata la lansare {status}.')

    def change_refresh_interval(self, val):
        self.refresh_interval_ore = int(val)
        self.save_config(refresh_interval_ore=self.refresh_interval_ore)
        self.log_msg(f'Interval soft-refresh schimbat la {val} ore.')

    def change_interval(self, val):
        self.interval_ore = int(val)
        self.save_config(interval_ore=self.interval_ore)
        self.log_msg(f'Interval schimbat la {val} ore.')

    # --- WATCHDOG (fir separat, monitorizeaza sanatatea achizitiei) ---
    def start_watchdog(self):
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True, name="CAEN-Watchdog")
        self._watchdog_thread.start()

    def _watchdog_loop(self):
        while True:
            time.sleep(WATCHDOG_CHECK_INTERVAL)
            if not self.running:
                continue
            try:
                self._watchdog_check()
            except Exception:
                self._append_crash_log(f"Watchdog exception:\n{traceback.format_exc()}")

    def _watchdog_check(self):
        now = time.time()

        # 1. Firul de achizitie a murit desi ar trebui sa ruleze -> repornire imediata
        if self._worker_thread is not None and not self._worker_thread.is_alive():
            self.log_msg('WATCHDOG: firul de achizitie s-a oprit neasteptat! Repornesc...')
            self.send_ntfy('Watchdog: worker oprit neasteptat, repornesc automat.',
                            'high', 'warning,arrows_counterclockwise')
            self._restart_worker()
            return

        stale = now - self.last_heartbeat
        if stale <= HEARTBEAT_STALE_THRESHOLD:
            return  # totul e OK, avem date recente

        self.log_msg(f'WATCHDOG: fara date reale de {int(stale)}s.')

        # 2. Fara date de prea mult timp -> incearca soft-refresh (o data la HEARTBEAT_STALE_THRESHOLD sec, nu spam)
        if now - self._last_watchdog_soft_refresh > HEARTBEAT_STALE_THRESHOLD:
            self.log_msg('WATCHDOG: incerc soft-refresh de recuperare.')
            self.send_ntfy(f'Watchdog: fara date de {int(stale)}s, incerc recuperare.', 'high', 'warning')
            self.soft_refresh_device()
            self._last_watchdog_soft_refresh = now

        # 3. Blocaj prelungit -> repornire completa a worker-ului + reboot daca e activat
        if stale > HEARTBEAT_STALE_THRESHOLD * 3 and now - self._last_watchdog_restart > HEARTBEAT_STALE_THRESHOLD * 3:
            self.log_msg('WATCHDOG: blocaj prelungit -> repornesc complet firul de achizitie.')
            self._restart_worker()
            self._last_watchdog_restart = now
            if self.reboot_enabled:
                self.reboot_device()

    def _restart_worker(self):
        self.worker_restart_count += 1
        self.save_config(worker_restart_count=self.worker_restart_count)
        try:
            self._ws.close()
        except Exception:
            pass
        self.erori_consecutive = 0
        self.consecutive_connect_failures = 0
        self.last_heartbeat = time.time()
        self._last_data_snapshot = None
        self._same_data_count = 0
        self._worker_thread = threading.Thread(target=self.worker_thread, daemon=True, name='CAEN-Worker')
        self._worker_thread.start()

    # --- Worker ---
    def worker_thread(self):
        self.last_heartbeat = time.time()
        while self.running:
            try:
                self._worker_cycle()
            except Exception:
                # orice eroare neprevazuta e prinsa aici, ca firul sa nu moara silentios
                err_text = traceback.format_exc()
                self._append_crash_log(f"Worker cycle exception:\n{err_text}")
                self.log_msg('EROARE NEASTEPTATA in ciclul de achizitie (vezi crash_log.txt).')
                self.erori_consecutive += 1
                self._handle_error("eroare neasteptata in ciclu")
                self._interruptible_sleep(RETRY_SLEEP)

        self._ws.close()
        self._update_status(False)
        self.log_msg('Worker oprit.')

    def _worker_cycle(self):
        cycle_start = time.time()
        self._cycle_count += 1

        if time.time() - self.last_file_time > (self.interval_ore * 3600):
            self.log_msg('Interval expirat. Se creeaza fisier nou...')
            self.cmd_create_new_file_only()

        if not self._ws.connected:
            try:
                self.log_msg('Conectare WS...')
                self._ws.connect()
                self.log_msg('WS conectat.')
                self._update_status(True)
                self.erori_consecutive = 0
                self.consecutive_connect_failures = 0
            except Exception as e:
                self.consecutive_connect_failures += 1
                self.erori_consecutive += 1
                self._update_status(False)
                backoff = min(CONNECT_BACKOFF_MAX,
                              CONNECT_BACKOFF_BASE * (2 ** (self.consecutive_connect_failures - 1)))
                self.log_msg(f'Connect esuat (incercarea {self.consecutive_connect_failures}), '
                             f'reincerc in {backoff}s: {e}')
                self._handle_error(f'Connect esuat: {str(e)[:60]}')
                self._interruptible_sleep(backoff)
                return

        # Refolosim buffer-ul existent (resetat in loc) in loc sa alocam un
        # dict + 4 liste noi la fiecare ciclu de 10s.
        temp_cycle = self._temp_cycle_buffer
        for i in range(4):
            temp_cycle[i][0] = temp_cycle[i][1] = temp_cycle[i][2] = temp_cycle[i][3] = 0.0
        data_ok = False

        for s_id in range(4):
            if not self.running:
                break
            try:
                self._ws.send(self._query_json[s_id])
                r = self._ws.recv()
                if r:
                    data = json.loads(r).get('data', {})
                    if 'counters' in data:
                        raw = [float(c['value']) for c in data['counters'][:4]]
                    else:
                        raw = [float(v) for v in data.get('values', [])[:4]]
                    if raw:
                        div = 0.9 if s_id <= 1 else 0.45
                        scaled = [round(v / div, 2) for v in raw]
                        while len(scaled) < 4:
                            scaled.append(0.0)
                        self.buffer_date[s_id] = scaled
                        temp_cycle[s_id] = scaled
                        data_ok = True
                time.sleep(0.1)
            except (websocket.WebSocketTimeoutException,
                    websocket.WebSocketConnectionClosedException,
                    socket.timeout, OSError) as e:
                self.log_msg(f'Eroare sectiune {s_id}: {type(e).__name__}')
                self._ws.close()
                self._update_status(False)
                self.erori_consecutive += 1
                self._handle_error(str(e)[:60])
                break
            except Exception as e:
                self.log_msg(f'Eroare neasteptata sectiune {s_id}: {e}')
                self._ws.close()
                self._update_status(False)
                self.erori_consecutive += 1
                self._handle_error(str(e)[:60])
                break

        if data_ok:
            self.erori_consecutive = 0
            self.last_heartbeat = time.time()  # heartbeat = am primit date REALE, nu doar conexiune OK

            # Detectie "raspunde dar e inghetat": aceleasi 16 valori repetate multe cicluri la rand
            snapshot = tuple(v for i in range(4) for v in temp_cycle[i])
            if snapshot == self._last_data_snapshot:
                self._same_data_count += 1
            else:
                self._same_data_count = 0
                self._last_data_snapshot = snapshot

            if self._same_data_count >= STALE_DATA_REPEAT_THRESHOLD:
                self.log_msg(f'ATENTIE: date identice de {self._same_data_count} cicluri la rand '
                             f'- posibil aparat blocat intern (raspunde dar nu mai actualizeaza).')
                self.send_ntfy('Date identice repetate - aparat posibil blocat intern.', 'high', 'warning')
                self.soft_refresh_device()
                self._same_data_count = 0

            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row = [ts]
            for i in range(4):
                row.extend(map(str, temp_cycle[i]))
            try:
                with open(os.path.join(self.target_folder, self.current_active_file), 'a') as f:
                    f.write(';'.join(row) + '\n')
            except Exception as e:
                self.log_msg(f'Eroare scriere CSV: {e}')
                self.send_ntfy(f'Nu pot scrie in fisierul CSV: {e}', 'high', 'warning')

        elapsed = time.time() - cycle_start
        self._interruptible_sleep(max(0.0, CYCLE_INTERVAL - elapsed))

        # gc.collect() fortat rar (nu la fiecare ciclu de 10s) - Python are deja
        # colectare generationala automata; a forta colectarea completa la
        # fiecare ciclu inseamna sute de mii de colectari inutile pe ani de
        # rulare, fara beneficiu real, doar consum extra de CPU.
        if self._cycle_count % GC_COLLECT_INTERVAL_CYCLES == 0:
            gc.collect()

    def _interruptible_sleep(self, seconds, step=0.2):
        end = time.time() + seconds
        while self.running and time.time() < end:
            time.sleep(min(step, end - time.time()))

    def _handle_error(self, msg):
        if self.erori_consecutive == 1:
            self.send_ntfy(f'Eroare: {msg}', 'urgent', 'rotating_light,skull')
        elif self.erori_consecutive == SOFT_REFRESH_ERROR_THRESHOLD:
            self.log_msg('Erori repetate -> incerc soft-refresh automat...')
            self.soft_refresh_device()
        elif self.erori_consecutive == MAX_CONSECUTIVE_ERRORS:
            self.send_ntfy('Aparatul nu mai transmite date corecte!', 'high', 'warning,no_entry')
            if self.reboot_enabled:
                self.log_msg('Soft-refresh nu a rezolvat -> reboot complet.')
                self.reboot_device()

    def _update_status(self, connected):
        def _do():
            if connected:
                self.lbl_status.configure(text='Conectat', text_color='#2ecc71')
            else:
                self.lbl_status.configure(text='Deconectat', text_color='#e74c3c')
        self.after(0, _do)

    # La fiecare PLOT_FULL_REDRAW_EVERY actualizari (~ la 30s, cu update la 2s)
    # facem un redraw complet cu relim/autoscale, ca axele sa se adapteze daca
    # datele au iesit din scala curenta. Intre timp folosim blitting (doar
    # liniile sunt redesenate, nu toata figura) - mult mai putin CPU pe o
    # aplicatie care redeseneaza graficul la infinit, zile la rand.
    PLOT_FULL_REDRAW_EVERY = 15

    def refresh_ui_loop(self):
        if self.running:
            for s_id, litera in self.nume_sectiuni.items():
                vals = self.buffer_date[s_id]
                for i, v in enumerate(vals):
                    self.ui_labels[litera][i].configure(text=f'{v:.2f}')
                    self.plot_data[litera][str(i + 1)].append(v)
            try:
                self._plot_redraw_counter += 1
                need_full_redraw = (not self._plot_backgrounds_ready) or \
                    (self._plot_redraw_counter % self.PLOT_FULL_REDRAW_EVERY == 0)

                if need_full_redraw:
                    for char, ax in self.axes.items():
                        for i in range(1, 5):
                            self.plot_lines[char][i - 1].set_ydata(self.plot_data[char][str(i)])
                        ax.relim()
                        ax.autoscale_view()
                    self.canvas.draw()
                    self._plot_backgrounds = {c: self.canvas.copy_from_bbox(a.bbox) for c, a in self.axes.items()}
                    self._plot_backgrounds_ready = True
                else:
                    for char, ax in self.axes.items():
                        self.canvas.restore_region(self._plot_backgrounds[char])
                        for i in range(1, 5):
                            self.plot_lines[char][i - 1].set_ydata(self.plot_data[char][str(i)])
                            ax.draw_artist(self.plot_lines[char][i - 1])
                        self.canvas.blit(ax.bbox)
            except Exception:
                # O eroare de randare matplotlib (rara, dar posibila dupa
                # multe ore de rulare) nu trebuie sa opreasca intreaga
                # aplicatie - o logam, continuam, si fortam un redraw complet
                # la urmatorul ciclu ca sa ne recuperam starea de blitting.
                self._append_crash_log(f"Eroare randare grafic:\n{traceback.format_exc()}")
                self._plot_backgrounds_ready = False

        # actualizeaza panoul de watchdog indiferent daca achizitia ruleaza
        secunde = int(time.time() - self.last_heartbeat) if self.running else 0
        if self.running:
            culoare = '#27ae60' if secunde < HEARTBEAT_STALE_THRESHOLD else '#e74c3c'
            self.lbl_heartbeat.configure(text=f'Ultima transmisie: {secunde}s in urma', text_color=culoare)
        else:
            self.lbl_heartbeat.configure(text='Ultima transmisie: -', text_color='#7f8c8d')
        self.lbl_restarts.configure(text=f'Restart-uri worker: {self.worker_restart_count}')

        self.after(2000, self.refresh_ui_loop)

    def start_acq(self):
        if self.running:
            return
        self.running = True
        self.erori_consecutive = 0
        self.consecutive_connect_failures = 0
        self.last_heartbeat = time.time()
        self._last_data_snapshot = None
        self._same_data_count = 0
        self.btn_start.configure(state='disabled')
        self.btn_stop.configure(state='normal')
        self.send_ntfy('Achizitie pornita.', 'default', 'arrow_forward,computer')
        self._worker_thread = threading.Thread(target=self.worker_thread, daemon=True, name='CAEN-Worker')
        self._worker_thread.start()

    def stop_acq(self):
        self.running = False
        self.btn_start.configure(state='normal')
        self.btn_stop.configure(state='disabled')
        self.send_ntfy('Achizitie oprita de utilizator.', 'default', 'stop_button,person')

    def cmd_choose_folder(self):
        f = filedialog.askdirectory()
        if f:
            self.target_folder = f
            self.save_config(last_path=f)

    def log_msg(self, m):
        ts = datetime.now().strftime('%H:%M:%S')
        self.after(0, self._log_insert, f'[{ts}] {m}\n')

    def _log_insert(self, text):
        self.txt_log.insert('end', text)
        self.txt_log.see('end')
        lines = int(self.txt_log.index('end-1c').split('.')[0])
        if lines > 500:
            self.txt_log.delete('1.0', f'{lines - 500}.0')

    def _on_close(self):
        self.running = False
        self._ws.close()
        try:
            plt.close(self.fig)  # elibereaza resursele matplotlib la iesire
        except Exception:
            pass
        self.destroy()


def _acquire_single_instance_lock():
    """Blocheaza un port local ca sa nu ruleze 2 copii ale aplicatiei simultan
    (ar scrie in acelasi CSV si ar trimite reboot-uri/refresh-uri in paralel)."""
    lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        lock_socket.bind(("127.0.0.1", SINGLE_INSTANCE_PORT))
        lock_socket.listen(1)
        return lock_socket
    except OSError:
        return None


if __name__ == '__main__':
    _lock = _acquire_single_instance_lock()
    if _lock is None:
        try:
            import tkinter as _tk
            _root = _tk.Tk()
            _root.withdraw()
            messagebox.showerror("CAEN Monitor",
                                  "Aplicatia ruleaza deja (alta instanta este deschisa).")
            _root.destroy()
        except Exception:
            print("Aplicatia ruleaza deja (alta instanta este deschisa).")
    else:
        app = CAENProMonitor()
        app.mainloop()

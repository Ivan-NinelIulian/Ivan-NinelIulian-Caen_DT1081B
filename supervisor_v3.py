import subprocess
import sys
import threading
import time
import os
import json
from datetime import datetime

try:
    import psutil
    HAVE_PSUTIL = True
except ImportError:
    HAVE_PSUTIL = False

# Numele fisierelor pe care le porneste supervisorul, fiecare ca proces
# separat, independent supravegheat.
APP_SCRIPT = "caen_monitor_v6_optimized.py"
REF_SCRIPT = "refresh_page1.py"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEVICE_CONFIG_FILE = os.path.join(BASE_DIR, "device_config.json")

# Config implicita pentru device_config.json, folosita si de aplicatia
# principala si de refresh_page1.py. Creata automat la prima pornire daca
# lipseste, ca totul sa functioneze din prima (chiar daca cu valori
# placeholder pana editezi adresa reala a aparatului).
DEFAULT_DEVICE_CONFIG = {
    "_comment": "Editeaza ip_aparat cu adresa reala a aparatului CAEN (IP-ul de pe Ethernet). "
                "refresh_url poate ramane null - se calculeaza automat ca http://<ip_aparat>/. "
                "restart_interval_hours = la cate ore isi da restart COMPLET pagina/browserul "
                "(refresh_page1.py). Prima data se face restart mereu la pornirea scriptului.",
    "ip_aparat": "localhost",
    "refresh_url": None,
    "refresh_headless": False,
    "restart_interval_hours": 2,
}

# Praguri de siguranta pentru crash-uri (separate pentru fiecare proces).
CRASH_LOOP_THRESHOLD = 20
BACKOFF_BASE = 5
BACKOFF_MAX = 120

# Cat timp asteptam un proces sa se inchida "elegant" (terminate) inainte
# sa il fortam (kill).
GRACEFUL_SHUTDOWN_TIMEOUT = 10

LOG_FILE = os.path.join(BASE_DIR, "supervisor_log.txt")
STATE_FILE = os.path.join(BASE_DIR, "supervisor_state.json")

# Fisiere separate in care redirectam stdout/stderr ale celor doua procese
# copil (util pt. diagnosticare - de ex. mesaje/erori Chrome/chromedriver
# care altfel s-ar pierde, supervisorul rulind de multe ori fara consola vizibila).
APP_OUTPUT_LOG = os.path.join(BASE_DIR, "app_output.log")
REF_OUTPUT_LOG = os.path.join(BASE_DIR, "refresh_output.log")
OUTPUT_LOG_MAX_BYTES = 5 * 1024 * 1024

# Rotatie log: cand fisierul depaseste limita, il arhivam si pornim unul nou.
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_SUFFIX = ".1"

_log_lock = threading.Lock()
_state_lock = threading.Lock()

# Eveniment global de oprire: setat la Ctrl+C, ca sa opreasca ambele
# thread-uri de supraveghere in mod curat.
stop_event = threading.Event()


def rotate_file_if_needed(path, max_bytes):
    try:
        if os.path.exists(path) and os.path.getsize(path) > max_bytes:
            backup = path + ".1"
            if os.path.exists(backup):
                os.remove(backup)
            os.replace(path, backup)
    except Exception:
        pass


def log(msg):
    with _log_lock:
        rotate_file_if_needed(LOG_FILE, LOG_MAX_BYTES)
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


def ensure_device_config():
    """Creeaza device_config.json cu valori implicite daca nu exista deja,
    ca aplicatia principala si scriptul de refresh sa aiba mereu un fisier
    valid de citit, chiar la prima rulare."""
    if os.path.exists(DEVICE_CONFIG_FILE):
        return
    try:
        with open(DEVICE_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_DEVICE_CONFIG, f, indent=2)
        log(f"Am creat {DEVICE_CONFIG_FILE} cu valori implicite. "
            f"EDITEAZA 'ip_aparat' cu adresa reala a aparatului CAEN cand esti conectat la el!")
    except Exception as e:
        log(f"Nu am putut crea device_config.json: {e}")


def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {"total_restarts_app": 0, "total_restarts_refresh": 0, "start_time": time.time()}


def save_state(state):
    with _state_lock:
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f)
            os.replace(tmp, STATE_FILE)
        except Exception:
            pass


def _kill_child_tree(pid):
    """Omoara toate procesele-copil (recursiv) ale unui proces, folosind
    psutil daca e disponibil. Esential pentru refresh_page1.py: Selenium
    porneste chromedriver, care la randul lui porneste procesul Chrome
    propriu-zis. Daca omoram doar procesul Python parinte (mai ales cu
    kill() brutal, sau cu terminate() pe Windows care oricum nu lasa loc
    pentru cleanup), chromedriver/Chrome raman orfane si se acumuleaza la
    fiecare restart - exact genul de scurgere care ar strica un sistem lasat
    sa ruleze zile/saptamani la rand."""
    if not HAVE_PSUTIL:
        return
    try:
        parent = psutil.Process(pid)
    except Exception:
        return
    try:
        children = parent.children(recursive=True)
    except Exception:
        children = []
    for child in children:
        try:
            child.kill()
        except Exception:
            pass


def terminate_process(proc, name):
    """Opreste un proces in mod sigur: terminate() cu timeout, apoi kill()
    daca nu raspunde - si, indiferent de rezultat, curata orice proces-copil
    ramas orfan (vezi _kill_child_tree)."""
    if proc is None or proc.poll() is not None:
        return

    pid = proc.pid
    children_snapshot = []
    if HAVE_PSUTIL:
        try:
            children_snapshot = psutil.Process(pid).children(recursive=True)
        except Exception:
            children_snapshot = []

    try:
        proc.terminate()
        proc.wait(timeout=GRACEFUL_SHUTDOWN_TIMEOUT)
        log(f"{name}: inchis normal (terminate).")
    except subprocess.TimeoutExpired:
        log(f"{name}: nu a raspuns la terminate(), fortez cu kill().")
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception as e:
            log(f"{name}: eroare la kill(): {e}")
    except Exception as e:
        log(f"{name}: eroare la oprire: {e}")
    finally:
        # Plasa de siguranta finala: chiar daca proc.terminate()/kill() a
        # "reusit" din perspectiva Python, s-ar putea sa fi lasat copii in
        # urma (Chrome/chromedriver). Ii omoram explicit pe cei detectati
        # inainte de oprire, plus o verificare suplimentara dupa.
        for child in children_snapshot:
            try:
                if child.is_running():
                    child.kill()
            except Exception:
                pass
        _kill_child_tree(pid)


def _open_output_log(path):
    rotate_file_if_needed(path, OUTPUT_LOG_MAX_BYTES)
    try:
        return open(path, "a", encoding="utf-8", errors="replace")
    except Exception:
        return subprocess.DEVNULL


def supraveghere_proces(script_path, nume_afisat, state_key, state, output_log_path):
    """Bucla generica de supraveghere pentru un proces: il porneste, asteapta
    sa se inchida/crape, il reporneste cu backoff exponential. Ruleaza
    intr-un thread separat, independent de celalalt proces supravegheat."""
    consecutive_fast_crashes = 0

    while not stop_event.is_set():
        start_time = time.time()
        log(f"[{nume_afisat}] Pornesc: {script_path}")

        proc = None
        out_f = _open_output_log(output_log_path)
        try:
            proc = subprocess.Popen(
                [sys.executable, "-u", script_path],
                cwd=BASE_DIR,  # cai relative (config, loguri, profil Chrome) raman stabile
                stdout=out_f,
                stderr=subprocess.STDOUT,
            )

            while proc.poll() is None and not stop_event.is_set():
                time.sleep(1)

            if stop_event.is_set():
                terminate_process(proc, nume_afisat)
                break

            exit_code = proc.returncode
            runtime = time.time() - start_time
            log(f"[{nume_afisat}] S-a oprit (cod: {exit_code}) dupa {int(runtime)}s.")

        except Exception as e:
            log(f"[{nume_afisat}] Eroare in timpul rularii: {e}")
            runtime = time.time() - start_time

        finally:
            terminate_process(proc, nume_afisat)
            if out_f not in (None, subprocess.DEVNULL):
                try:
                    out_f.close()
                except Exception:
                    pass

        state[state_key] = state.get(state_key, 0) + 1
        save_state(state)

        if runtime < CRASH_LOOP_THRESHOLD:
            consecutive_fast_crashes += 1
        else:
            consecutive_fast_crashes = 0

        backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** max(0, consecutive_fast_crashes - 1)))

        if consecutive_fast_crashes >= 5:
            log(f"[{nume_afisat}] ATENTIE: {consecutive_fast_crashes} crash-uri rapide consecutive. "
                f"Posibila problema persistenta (config, retea, permisiuni).")

        log(f"[{nume_afisat}] Repornesc in {backoff}s... (total restart-uri: {state[state_key]})")

        stop_event.wait(timeout=backoff)


def main():
    app_path = os.path.join(BASE_DIR, APP_SCRIPT)
    ref_path = os.path.join(BASE_DIR, REF_SCRIPT)

    if not os.path.exists(app_path) or not os.path.exists(ref_path):
        log(f"EROARE: Nu gasesc unul dintre fisiere.\nVerifica:\n1. {app_path}\n2. {ref_path}")
        return

    if not HAVE_PSUTIL:
        log("ATENTIE: pachetul 'psutil' nu este instalat (pip install psutil). "
            "Fara el, supervisorul nu poate garanta ca omoara procesele Chrome orfane "
            "la restart - recomandat pentru rulare stabila pe termen lung.")

    ensure_device_config()

    state = load_state()
    log(f"Supervisor pornit. Restart-uri totale pana acum: "
        f"aplicatie={state.get('total_restarts_app', 0)}, "
        f"refresh={state.get('total_restarts_refresh', 0)}.")

    # --- ORDINE DE PORNIRE MODIFICATA: refresh_page1.py primul, apoi
    # caen_monitor_v5_5_optimized.py al doilea. ---
    thread_ref = threading.Thread(
        target=supraveghere_proces,
        args=(ref_path, "Script refresh", "total_restarts_refresh", state, REF_OUTPUT_LOG),
        daemon=True,
    )
    thread_app = threading.Thread(
        target=supraveghere_proces,
        args=(app_path, "Aplicatie principala", "total_restarts_app", state, APP_OUTPUT_LOG),
        daemon=True,
    )

    thread_ref.start()
    thread_app.start()

    try:
        while thread_app.is_alive() or thread_ref.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        log("Supervisor oprit manual (Ctrl+C). Opresc procesele copil...")
        stop_event.set()
        thread_app.join(timeout=GRACEFUL_SHUTDOWN_TIMEOUT + 5)
        thread_ref.join(timeout=GRACEFUL_SHUTDOWN_TIMEOUT + 5)


if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            log("Supervisor oprit manual (Ctrl+C).")
            break
        except Exception as e:
            log(f"EROARE CRITICA in supervisor (neasteptata): {e}. Repornesc bucla principala in 10s.")
            time.sleep(10)

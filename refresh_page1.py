import json
import os
import sys
import time
import signal
import atexit
import traceback
import threading
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException

# --- CONFIGURARE ȘI CĂI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "device_config.json")
LOG_FILE = os.path.join(BASE_DIR, "refresh_log.txt")
CHROME_PROFILE_DIR = os.path.join(BASE_DIR, "chrome_refresh_profile")

LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per fișier de log

DEFAULT_CONFIG = {
    "ip_aparat": "192.168.50.244",
    "refresh_url": None,
    "refresh_headless": False,
    "login_enabled": True,
    "login_password": "password",
    "login_password_selector": "#login_password", 
    "login_submit_selector": ".input-group-btn button.btn", 
    "logged_in_indicator_selector": "a[href*='logout'], .logged-in, #main_content, .dashboard", # Modifică cu selectorul real de după login dacă e cazul
}

def load_config():
    cfg = dict(DEFAULT_CONFIG)
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
    except Exception:
        pass
    return cfg

CONFIG = load_config()
IP_APARAT = CONFIG.get("ip_aparat") or "127.0.0.1"
URL = CONFIG.get("refresh_url") or f"http://{IP_APARAT}/"
HEADLESS = bool(CONFIG.get("refresh_headless", False))

LOGIN_ENABLED = bool(CONFIG.get("login_enabled", True))
LOGIN_PASSWORD = CONFIG.get("login_password", "")
LOGIN_PASS_SELECTOR = CONFIG.get("login_password_selector", "#login_password")
LOGIN_SUBMIT_SELECTOR = CONFIG.get("login_submit_selector", ".input-group-btn button.btn")
LOGGED_IN_SELECTOR = CONFIG.get("logged_in_indicator_selector", "")

PAGE_LOAD_TIMEOUT = 30
WATCHDOG_TIMEOUT = 45  # Secunde maxime permise pentru o acțiune Selenium blocată

# --- LOGGING AVANSAT ȘI ROTATIV ---
_log_lock = threading.Lock()

def _rotate_log_if_needed():
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_BYTES:
            backup = LOG_FILE + ".1"
            if os.path.exists(backup):
                os.remove(backup)
            os.replace(LOG_FILE, backup)
    except Exception:
        pass

def log_msg(level, msg):
    with _log_lock:
        _rotate_log_if_needed()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        line = f"[{timestamp}] [{level.upper()}] {msg}"
        print(line)
        sys.stdout.flush()
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

# --- MANAGEMENTUL DRIVERULUI ȘI PLASA DE SIGURANȚĂ ---
class BrowserManager:
    def __init__(self):
        self.driver = None
        self.lock = threading.Lock()
        self.last_activity = time.time()
        self._watchdog_active = False

    def build_driver(self):
        with self.lock:
            self.close_driver()
            log_msg("info", "Se inițializează o nouă instanță de Chrome Driver...")
            
            options = Options()
            options.add_argument(f"--user-data-dir={CHROME_PROFILE_DIR}")
            options.add_argument("--no-first-run")
            options.add_argument("--no-default-browser-check")
            options.add_argument("--disable-session-crashed-bubble")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-popup-blocking")
            options.add_argument("--mute-audio")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            
            if HEADLESS:
                options.add_argument("--headless=new")
                
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)

            try:
                self.driver = webdriver.Chrome(options=options)
                self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                self.driver.set_script_timeout(PAGE_LOAD_TIMEOUT)
                self.update_activity()
                log_msg("info", "Chrome Driver a fost pornit cu succes.")
                return True
            except Exception as e:
                log_msg("critical", f"Eroare fatală la crearea driverului: {type(e).__name__}: {e}")
                self.driver = None
                return False

    def update_activity(self):
        self.last_activity = time.time()

    def close_driver(self):
        if self.driver:
            log_msg("info", "Se închide instanța curentă de Chrome...")
            try:
                self.driver.quit()
            except Exception as e:
                log_msg("debug", f"Excepție la quit() (normal dacă e deja mort): {e}")
            finally:
                self.driver = None

    def start_watchdog(self):
        self._watchdog_active = True
        t = threading.Thread(target=self._watchdog_loop, daemon=True, name="Chrome-Watchdog")
        t.start()

    def _watchdog_loop(self):
        log_msg("info", "Watchdog intern pentru Chrome pornit.")
        while self._watchdog_active:
            time.sleep(5)
            if self.driver:
                stale_time = time.time() - self.last_activity
                if stale_time > WATCHDOG_TIMEOUT:
                    log_msg("warning", f"Watchdog detectat: Zero activitate Selenium de {int(stale_time)}s. Forțare restart driver...")
                    # Închidem driverul direct din alt fir pentru a debloca apelul agățat în Selenium
                    self.close_driver()

manager = BrowserManager()

def cleanup():
    manager._watchdog_active = False
    manager.close_driver()

atexit.register(cleanup)
try:
    signal.signal(signal.SIGTERM, lambda s, f: (cleanup(), sys.exit(0)))
    signal.signal(signal.SIGINT, lambda s, f: (cleanup(), sys.exit(0)))
except Exception:
    pass

# --- LOGICĂ INTELIGENTĂ DE PAGINĂ ȘI LOGARE ---
def is_already_logged_in(driver):
    """Verifică dacă sesiunea este deja activă pentru a nu reintroduce parola inutil."""
    if not LOGGED_IN_SELECTOR:
        return False
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, LOGGED_IN_SELECTOR)
        if len(elements) > 0 and elements[0].is_displayed():
            log_msg("info", "Sesiune existentă detectată. Pagina este deja autentificată.")
            return True
    except Exception:
        pass
    return False

def check_field_value(driver, selector, expected_value):
    """Verifică riguros dacă valoarea a fost într-adevăr scrisă în input."""
    try:
        field = driver.find_element(By.CSS_SELECTOR, selector)
        actual_value = field.get_attribute("value")
        return actual_value == expected_value
    except Exception:
        return False

def execute_smart_login(driver):
    if not LOGIN_ENABLED:
        return True

    log_msg("info", "Se verifică starea curentă a autentificării...")
    manager.update_activity()
    
    # Așteptare inteligentă a paginii (să aibă ori campul de login ori indicatorul de logat)
    wait = WebDriverWait(driver, 15)
    try:
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    except TimeoutException:
        log_msg("warning", "Pagina nu a raportat readyState complete, dar continuăm verificarea elementelor.")

    if is_already_logged_in(driver):
        return True

    log_msg("info", "Pornire procedură de login 100% fiabilă...")
    try:
        # Așteaptă prezența câmpului de parolă
        pass_field = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, LOGIN_PASS_SELECTOR)))
        
        # Încercări succesive de scriere cu verificare imediată (fiabilitate strictă)
        for input_attempt in range(1, 4):
            pass_field.clear()
            pass_field.send_keys(LOGIN_PASSWORD)
            manager.update_activity()
            
            if check_field_value(driver, LOGIN_PASS_SELECTOR, LOGIN_PASSWORD):
                log_msg("info", f"Verificare reușită: Parola a fost introdusă corect în câmp (încercarea {input_attempt}).")
                break
            else:
                log_msg("warning", f"Eroare scriere: Parola nu s-a completat corect la încercarea {input_attempt}. Reîncerc...")
                time.sleep(1)
        else:
            raise RuntimeError("Nu s-a putut confirma scrierea parolei în DOM după 3 încercări consecutive.")

        # Click pe buton sau submit inteligent
        if LOGIN_SUBMIT_SELECTOR:
            submit_btn = driver.find_element(By.CSS_SELECTOR, LOGIN_SUBMIT_SELECTOR)
            submit_btn.click()
        else:
            pass_field.send_keys(webdriver.Keys.RETURN)
            
        log_msg("info", "Formularul de autentificare a fost trimis. Se așteaptă confirmarea încărcării...")
        manager.update_activity()
        
        # Așteaptă stabilizarea paginii după login (dispariția câmpului de parolă sau apariția indicatorului de login)
        if LOGGED_IN_SELECTOR:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, LOGGED_IN_SELECTOR)))
        else:
            wait.until(EC.staleness_of(pass_field))
            
        log_msg("info", "Autentificare finalizată și validată cu succes.")
        return True

    except TimeoutException:
        log_msg("error", "Timeout la autentificare. Câmpul de parolă sau indicatorul post-login nu au apărut.")
        return False
    except Exception as e:
        log_msg("error", f"Eroare neașteptată în timpul ferestrei de login: {type(e).__name__}: {e}")
        return False

# --- CICLUL PRINCIPAL ---
def run_pipeline():
    """Rulează un ciclu curat de încărcare, login și verificare."""
    if not manager.driver:
        if not manager.build_driver():
            return False

    manager.update_activity()
    log_msg("info", f"Se accesează URL-ul aparatului: {URL}")
    
    try:
        manager.driver.get(URL)
    except WebDriverException as e:
        log_msg("error", f"Eroare la conectare (Aparatul nu răspunde la rețea): {type(e).__name__}")
        return False

    # Așteptare inteligentă încărcare structură DOM de bază
    try:
        WebDriverWait(manager.driver, PAGE_LOAD_TIMEOUT).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        log_msg("info", "Pagina a fost încărcată complet în browser.")
    except TimeoutException:
        log_msg("warning", "Pagina a depășit timpul standard de încărcare (Timeout), încercăm continuarea procedurii.")

    # Execuție Login
    if not execute_smart_login(manager.driver):
        return False

    return True

def main():
    log_msg("info", "=========================================================")
    log_msg("info", f"Pornire script refresh_page1 optimizat în mod stabil. Rulare non-stop activă.")
    log_msg("info", f"Setări: Headless={HEADLESS} | URL={URL} | Login={LOGIN_ENABLED}")
    log_msg("info", "=========================================================")

    manager.start_watchdog()
    
    consecutive_failures = 0
    
    while True:
        cycle_start = time.time()
        success = False
        
        try:
            success = run_pipeline()
        except Exception as e:
            log_msg("critical", f"Excepție neprevăzută în bucla principală: {type(e).__name__}: {e}\n{traceback.format_exc()}")
            success = False

        if success:
            consecutive_failures = 0
            log_msg("info", "Ciclu finalizat cu succes. Starea browserului este stabilă.")
        else:
            consecutive_failures += 1
            log_msg("warning", f"Eroare ciclu. Total defecțiuni consecutive: {consecutive_failures}")
            
            # Restart driver DOAR DACĂ CHIAR ESTE NEVOIE (după eșecuri clare de pipeline)
            # Evită blocajele Selenium prin distrugerea proceselor vechi zombie și recrearea completă a contextului
            log_msg("info", "Inițiere restart preventiv al driverului pentru eliminarea blocajelor Selenium...")
            manager.close_driver()
            
            # Reîncercare automată la autentificare / conectare cu sleep adaptat scurt (fără sleep-uri inutile)
            backoff = min(30, 5 * consecutive_failures)
            log_msg("info", f"Se așteaptă reîncercarea automată în {backoff} secunde...")
            
            # Sleep divizat, reactiv la întreruperi
            sleep_end = time.time() + backoff
            while time.time() < sleep_end:
                time.sleep(0.5)
            continue

        # Funcționare continuă: Scriptul rămâne activ pe pagină făcând doar verificări de mentenanță la intervale regulate
        # Fără sleep-uri oarbe lungi în interiorul interacțiunilor DOM. Aici stă paginat non-stop.
        maintenance_interval = 15
        next_check = time.time() + maintenance_interval
        
        while time.time() < next_check:
            time.sleep(1)
            # Verificare periodică rapidă dacă driverul este încă în viață și răspunde la comenzi de bază
            if time.time() - manager.last_activity > maintenance_interval:
                try:
                    if manager.driver:
                        # Un apel extrem de ușor către motorul Chrome ca să confirmăm că procesul nu e înghețat
                        manager.driver.execute_script("return 1;")
                        manager.update_activity()
                except Exception:
                    log_msg("warning", "Instanța Chrome nu mai răspunde la comenzi de bază. Ieșire din starea de veghe.")
                    break

if __name__ == "__main__":
    main()
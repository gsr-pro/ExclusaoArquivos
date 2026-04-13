# ExcluiDestino_Turbo.py
# Versão TURBO: exclusão segura + paralelismo + logs enxutos
# - Pipeline produtor-consumidor (os.scandir + Queue)
# - Auto-tuning de threads baseado em CPU
# - Sobrescrita opcional (secure delete) antes de remover
# - Log CSV de erros (thread-safe)
# - Limpeza de pastas vazias no final

import os
import logging
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import time
import errno
import stat

# ========================
# CONFIGURAÇÕES GERAIS
# ========================

# Quantas threads de exclusão usar (auto-tuning)
CPU = os.cpu_count() or 4
MAX_WORKERS_DELETE = min(32, max(4, 2 * CPU))  # 4..32

# Log periódico de progresso (segundos)
HEARTBEAT_SECONDS = 30

# Exclusão segura:
# - SECURE_PASSES = 1 -> sobrescreve o arquivo 1x com dados aleatórios antes de remover
# - SECURE_PASSES = 0 -> não sobrescreve (apenas remove -> mais rápido)
SECURE_PASSES = 1

# Tamanho do chunk de sobrescrita (bytes)
OVERWRITE_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB

# DRY_RUN = True -> não apaga nada, apenas loga o que faria
DRY_RUN = False

# Pastas a ignorar (para não apagar, ex.: logs do próprio script)
SKIP_DIRS = {
    "logs",
    "__macosx",
}

# Arquivos a ignorar
IGNORES_FILENAME = {"thumbs.db", ".ds_store", "desktop.ini"}

# ========================
# LOG (console)
# ========================
def setup_logging(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

logger = logging.getLogger(__name__)
setup_logging(debug=False)

logger.info(f"🧠 Auto-tuning: CPU={CPU} | MAX_WORKERS_DELETE={MAX_WORKERS_DELETE} | SECURE_PASSES={SECURE_PASSES} | DRY_RUN={DRY_RUN}")

# ========================
# Error log (CSV) - thread-safe
# ========================
ERROR_LOG = []
ERROR_LOCK = threading.Lock()
ERROR_LOG_DIR = None
ERROR_LOG_PATH = None

def init_error_log(destino_base: str):
    """Define caminho do CSV de erros e garante a pasta logs/ dentro do destino."""
    global ERROR_LOG_DIR, ERROR_LOG_PATH, ERROR_LOG
    ERROR_LOG_DIR = os.path.join(destino_base, "logs")
    os.makedirs(ERROR_LOG_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    ERROR_LOG_PATH = os.path.join(ERROR_LOG_DIR, f"erros_exclusao_{stamp}.csv")
    ERROR_LOG = []

def log_error(etapa: str, acao: str, arquivo: str, mensagem: str):
    row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "etapa": etapa,
        "acao": acao,
        "arquivo": arquivo or "",
        "erro": (mensagem or "").replace("\n", " ").strip(),
    }
    with ERROR_LOCK:
        ERROR_LOG.append(row)

def save_error_log():
    if not ERROR_LOG_PATH or not ERROR_LOG:
        return
    campos = ["timestamp", "etapa", "acao", "arquivo", "erro"]
    try:
        with open(ERROR_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=campos, delimiter=";")
            w.writeheader()
            w.writerows(ERROR_LOG)
        logger.info(f"🧾 Log de erros salvo: {ERROR_LOG_PATH}")
    except Exception as e:
        logger.warning(f"❌ Falha ao salvar log de erros: {e}")

# ========================
# Helpers de diretório
# ========================
def should_skip_dir(dir_name: str) -> bool:
    d = (dir_name or "").lower()
    return d in SKIP_DIRS

# ========================
# Exclusão segura
# ========================
def secure_overwrite(path: str, passes: int = 1):
    """
    Sobrescreve o arquivo com dados aleatórios 'passes' vezes.
    Observação: em SSDs/sistemas com journaling, não há garantia absoluta de wipe físico.
    """
    if passes <= 0:
        return

    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        return
    except PermissionError as e:
        raise e
    except Exception as e:
        # Se não conseguir tamanho, tenta assim mesmo.
        size = None
        log_error("overwrite", "stat", path, str(e))

    if size is None or size <= 0:
        return

    try:
        with open(path, "r+b", buffering=0) as f:
            for p in range(passes):
                f.seek(0)
                remaining = size
                while remaining > 0:
                    chunk = min(OVERWRITE_CHUNK_SIZE, remaining)
                    f.write(os.urandom(chunk))
                    remaining -= chunk
                # Garante flush no disco
                f.flush()
                os.fsync(f.fileno())
    except FileNotFoundError:
        # já foi removido em outra thread?
        return
    except PermissionError as e:
        raise e
    except Exception as e:
        # Se falhar, loga e segue para tentativa de remoção mesmo assim
        log_error("overwrite", "write", path, str(e))

def _force_delete(path: str):
    """
    Remove arquivo tentando ajustar permissões se necessário.
    """
    try:
        os.remove(path)
    except PermissionError:
        try:
            # Tenta liberar escrita/leitura
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            os.remove(path)
        except Exception:
            raise

def delete_file(path: str):
    """
    Exclusão de arquivo com opção de sobrescrita segura.
    """
    if DRY_RUN:
        logger.info(f"[DRY_RUN] Iria excluir: {path}")
        return

    # Sobrescrita segura (se configurada)
    if SECURE_PASSES > 0:
        secure_overwrite(path, SECURE_PASSES)

    # Remoção física
    _force_delete(path)

# ========================
# Scanner (produtor)
# ========================
def listar_arquivos_para_excluir(root: str, q: Queue):
    """
    Lista arquivos recursivamente e coloca na fila para exclusão.
    Ignora pastas em SKIP_DIRS e alguns arquivos de sistema.
    """
    stack = [root]
    total_listados = 0
    last_hb = time.monotonic()

    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    name = e.name
                    if e.is_dir(follow_symlinks=False):
                        if not should_skip_dir(name):
                            stack.append(e.path)
                    elif e.is_file(follow_symlinks=False):
                        low = name.lower()
                        if low in IGNORES_FILENAME:
                            continue
                        q.put(e.path)
                        total_listados += 1

                        now = time.monotonic()
                        if now - last_hb >= HEARTBEAT_SECONDS:
                            logger.info(f"🔎 Listados: ~{total_listados:,} arquivos para exclusão")
                            last_hb = now
        except PermissionError as e:
            log_error("listar", "scan", d, str(e))
            continue
        except FileNotFoundError:
            # pasta sumiu durante o scan; ignora
            continue
        except Exception as e:
            log_error("listar", "scan", d, str(e))
            continue

    return total_listados

# ========================
# Worker de exclusão (consumidor)
# ========================
def worker_excluir(q: Queue, stats: dict, lock: threading.Lock):
    while True:
        path = q.get()
        if path is None:
            q.task_done()
            break

        try:
            delete_file(path)
            with lock:
                stats["deleted"] += 1
        except FileNotFoundError:
            # Arquivo já não existe; considera como ok
            with lock:
                stats["deleted"] += 1
        except PermissionError as e:
            log_error("excluir", "remove", path, f"permissao: {e}")
            with lock:
                stats["errors"] += 1
        except OSError as e:
            # Ex.: EBUSY, EACCES e outros
            log_error("excluir", "remove", path, f"oserror {e.errno}: {e}")
            with lock:
                stats["errors"] += 1
        except Exception as e:
            log_error("excluir", "remove", path, str(e))
            with lock:
                stats["errors"] += 1
        finally:
            q.task_done()

# ========================
# Limpeza de pastas vazias
# ========================
def limpar_pastas_vazias(destino_base: str):
    """
    Remove pastas vazias (exceto as ignoradas) após a exclusão.
    """
    for root, dirs, files in os.walk(destino_base, topdown=False):
        # não tentar remover as pastas de logs do script
        if should_skip_dir(os.path.basename(root)):
            continue
        if os.path.abspath(root) == os.path.abspath(destino_base):
            continue
        try:
            if not os.listdir(root):
                os.rmdir(root)
                logger.debug(f"🧹 Pasta vazia removida: {root}")
        except Exception as e:
            logger.debug(f"⚠️ Não foi possível remover {root}: {e}")

# ========================
# Processo principal de exclusão
# ========================
def excluir_recursivo(destino_base: str, max_workers: int = MAX_WORKERS_DELETE):
    if not os.path.isdir(destino_base):
        raise ValueError("Caminho de DESTINO inválido.")

    init_error_log(destino_base)

    logger.info("🚀 Iniciando exclusão recursiva de arquivos no DESTINO")
    if DRY_RUN:
        logger.info("🔍 MODO DRY_RUN ATIVO: nada será realmente apagado, apenas logado.")

    q = Queue(maxsize=10000)
    stats = {"deleted": 0, "errors": 0}
    lock = threading.Lock()

    # Inicia workers
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for _ in range(max_workers):
            ex.submit(worker_excluir, q, stats, lock)

        # Produtor faz o scan
        total_listados = listar_arquivos_para_excluir(destino_base, q)

        # Envia sentinelas para workers
        for _ in range(max_workers):
            q.put(None)

        # Aguarda acabar tudo
        q.join()

    logger.info(f"🏁 Exclusão concluída: listados ~{total_listados:,}, excluídos {stats['deleted']:,}, erros {stats['errors']:,}")

    # Limpa pastas vazias
    logger.info("🧹 Limpando pastas vazias...")
    limpar_pastas_vazias(destino_base)

    # Salva log de erros
    save_error_log()
    logger.info("✅ Finalizado.")

# ========================
# MAIN
# ========================
if __name__ == "__main__":
    try:
        destino = input("📂 Caminho do DESTINO (será LIMPO, apenas arquivos): ").strip()
        if not destino:
            raise ValueError("Caminho de DESTINO não informado.")

        print("\n⚠️ ATENÇÃO: TODOS os ARQUIVOS dentro deste DESTINO serão apagados (pastas permanecem, mas vazias).")
        print(f"DESTINO: {destino}")
        print(f"Secure delete (sobrescrita): {SECURE_PASSES} passe(s)")
        print(f"DRY_RUN (teste): {DRY_RUN}")
        confirm = input("Digite 'SIM' para confirmar: ").strip().upper()

        if confirm != "SIM":
            print("Operação cancelada pelo usuário.")
        else:
            excluir_recursivo(destino, max_workers=MAX_WORKERS_DELETE)

    except KeyboardInterrupt:
        logger.warning("🛑 Interrompido pelo usuário (CTRL+C). Salvando log de erros...")
        try:
            save_error_log()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        try:
            save_error_log()
        except Exception:
            pass

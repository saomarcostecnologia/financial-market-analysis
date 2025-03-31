# scripts/cleanup_project.py
"""
Script para limpar arquivos desnecessários do projeto.
"""
import os
import shutil
import fnmatch
import argparse
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("cleanup")

# Diretórios que sempre devem ser preservados
ESSENTIAL_DIRS = [
    "src",
    "tests",
    "scripts",
    "docs",
    "infrastructure"
]

# Arquivos que sempre devem ser preservados
ESSENTIAL_FILES = [
    "README.md",
    "LICENSE",
    ".gitignore",
    "requirements.txt",
    ".env.example",
    "setup.py"
]

# Padrões de arquivos a serem removidos
CLEANUP_PATTERNS = [
    "*.pyc",
    "__pycache__",
    "*.log",
    ".DS_Store",
    "*.bak",
    "*.tmp",
    ".coverage",
    "htmlcov",
    ".pytest_cache",
    "*.egg-info",
    "build",
    "dist",
    ".vscode"
]

def is_essential_path(path, root_dir):
    """Verifica se o caminho é essencial e deve ser preservado."""
    rel_path = os.path.relpath(path, root_dir)
    
    # Verificar se é um diretório essencial ou está dentro de um
    for essential_dir in ESSENTIAL_DIRS:
        if rel_path == essential_dir or rel_path.startswith(f"{essential_dir}/"):
            return True
    
    # Verificar se é um arquivo essencial
    for essential_file in ESSENTIAL_FILES:
        if rel_path == essential_file:
            return True
    
    return False

def matches_cleanup_pattern(path):
    """Verifica se o caminho corresponde a um padrão de limpeza."""
    basename = os.path.basename(path)
    for pattern in CLEANUP_PATTERNS:
        if fnmatch.fnmatch(basename, pattern):
            return True
    return False

def cleanup_empty_directories(root_dir):
    """Remove diretórios vazios recursivamente."""
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Pular diretórios essenciais
        if is_essential_path(dirpath, root_dir):
            continue
        
        # Verificar se o diretório está vazio
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
                logger.info(f"Removido diretório vazio: {dirpath}")
            except Exception as e:
                logger.error(f"Erro ao remover diretório vazio {dirpath}: {str(e)}")

def cleanup_project(root_dir, dry_run=False, aggressive=False):
    """
    Limpa arquivos desnecessários do projeto.
    
    Args:
        root_dir: Diretório raiz do projeto
        dry_run: Se True, apenas simula a limpeza sem remover arquivos
        aggressive: Se True, remove também arquivos que não correspondem aos padrões essenciais
    """
    logger.info(f"Iniciando limpeza do projeto em: {root_dir}")
    logger.info(f"Modo: {'Simulação' if dry_run else 'Real'}")
    logger.info(f"Modo agressivo: {'Sim' if aggressive else 'Não'}")
    
    removed_count = 0
    
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Processar arquivos
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            
            # Verificar se deve remover
            should_remove = matches_cleanup_pattern(filepath)
            
            # No modo agressivo, remover qualquer arquivo não essencial
            if aggressive and not is_essential_path(filepath, root_dir):
                should_remove = True
            
            if should_remove:
                if dry_run:
                    logger.info(f"[Simulação] Removeria: {filepath}")
                else:
                    try:
                        os.remove(filepath)
                        logger.info(f"Removido: {filepath}")
                        removed_count += 1
                    except Exception as e:
                        logger.error(f"Erro ao remover {filepath}: {str(e)}")
        
        # Processar diretórios
        for dirname in dirnames:
            dirpath_full = os.path.join(dirpath, dirname)
            
            # Verificar se deve remover
            should_remove = matches_cleanup_pattern(dirpath_full)
            
            # No modo agressivo, remover qualquer diretório não essencial
            if aggressive and not is_essential_path(dirpath_full, root_dir):
                should_remove = True
            
            if should_remove:
                if dry_run:
                    logger.info(f"[Simulação] Removeria diretório: {dirpath_full}")
                else:
                    try:
                        shutil.rmtree(dirpath_full)
                        logger.info(f"Removido diretório: {dirpath_full}")
                        removed_count += 1
                    except Exception as e:
                        logger.error(f"Erro ao remover diretório {dirpath_full}: {str(e)}")
    
    # Remover diretórios vazios
    if not dry_run:
        cleanup_empty_directories(root_dir)
    
    logger.info(f"Limpeza concluída. {'Simulação de ' if dry_run else ''}Remoção de {removed_count} itens.")

def parse_args():
    """Analisa os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description='Limpa arquivos desnecessários do projeto.')
    parser.add_argument('--root', type=str, default='.', help='Diretório raiz do projeto')
    parser.add_argument('--dry-run', action='store_true', help='Apenas simular a limpeza sem remover arquivos')
    parser.add_argument('--aggressive', action='store_true', help='Modo agressivo: remover tudo que não é essencial')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    cleanup_project(args.root, args.dry_run, args.aggressive)
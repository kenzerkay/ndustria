import os
from pathlib import Path

# Default "cache" and config location
NCD_ENV="NDUSTRIA_CACHE_DIR"
default_config_location = os.path.expanduser("~/.ndustria_config")
default_cache_location = os.path.expanduser("~/.ndustria_cache")

# Add files to path 
PY_LINE = 'export PYTHONPATH="$PYTHONPATH:$HOME"'
PATH_LINE = 'export PATH="$PATH:$HOME/ndustria/bin"'
FISH_PY_LINE = 'set -gx PYTHONPATH $PYTHONPATH $HOME'
FISH_PATH_LINE = 'set -gx PATH $PATH $HOME/ndustria/bin'

def detect_shell():
    shell = os.environ.get("SHELL", "")
    if shell.endswith("fish"):
        return "fish"
    elif shell.endswith(("bash", "zsh", "sh", "dash", "ksh")):
        return "posix"
    else:
        return "unknown"

def append_if_missing(file_path, lines):
    if not file_path.exists():
        file_path.touch()
    content = file_path.read_text()
    with file_path.open("a") as f:
        for line in lines:
            if line not in content:
                f.write(f"\n{line}")
                print(f"[INFO] Added to {file_path.name}: {line}")
            else:
                print(f"[INFO] Already in {file_path.name}: {line}")
    
def main():
    shell_type = detect_shell()
    home = Path.home()

    if shell_type == "fish":
        config_file = home / ".config" / "fish" / "config.fish"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        append_if_missing(config_file, [FISH_PY_LINE, FISH_PATH_LINE])

    elif shell_type == "posix":
        for rc in [".bashrc", ".zshrc", ".profile"]:
            rc_path = home / rc
            if rc_path.exists():
                append_if_missing(rc_path, [PY_LINE, PATH_LINE])
                break
        else:
            rc_path = home / ".bashrc"
            append_if_missing(rc_path, [PY_LINE, PATH_LINE])

    else:
        print("[WARNING] Unknown shell. Please manually add these lines to your shell's startup script:")
        print(PY_LINE)
        print(PATH_LINE)

    cache_dir = input(f"Please let me know where you'd like ndustria to keep cached files (default: {default_cache_location})")
    cache_dir = cache_dir.strip()
    if cache_dir == "":
            cache_dir = os.path.expanduser(default_cache_location)

    if os.path.exists(cache_dir):
        print(f"Found ndustria cache at {cache_dir}")
    else:
        print(f"Creating new directory at {cache_dir}")
        os.mkdir(cache_dir)

        print(f"Creating config file at {default_config_location}")
        with open(default_config_location, "w") as ef:
            ef.write(f"{NCD_ENV}={cache_dir}\n")

    print("ndustria is ready to run!")

if __name__ == "__main__":
    main()


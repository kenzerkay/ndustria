import os
from pathlib import Path

def main():
    home = str(Path.home())
    bashrc = Path(home) / ".bashrc"

    line1 = 'export PYTHONPATH="$PYTHONPATH:$HOME"'
    line2 = 'export PATH="$PATH:$HOME/ndustria/bin"'

    updated = False

    with open(bashrc, "a+") as f:
        f.seek(0)
        contents = f.read()
        if line1 not in contents:
            f.write(f"\n{line1}\n")
            print("✅ Added PYTHONPATH to .bashrc")
            updated = True
        if line2 not in contents:
            f.write(f"{line2}\n")
            print("✅ Added ndustria/bin to PATH in .bashrc")
            updated = True

    if updated:
        print("💡 Please run: source ~/.bashrc")
    else:
        print("⚠️  Lines already present — no changes made.")
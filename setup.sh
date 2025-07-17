#!/bin/bash

LINE1="# Add ndustria to Path via .bashrc"
LINE2='export PYTHONPATH="$PYTHONPATH:$HOME"'
LINE3='export PATH="$PATH:$HOME/ndustria/bin"'

if ! grep -Fxq "$LINE1" ~/.bashrc; then
    echo "$LINE1" >> ~/.bashrc
fi

if ! grep -Fxq "$LINE2" ~/.bashrc; then
    echo "$LINE2" >> ~/.bashrc
    echo "✅ Added PYTHONPATH to ~/.bashrc"
fi

if ! grep -Fxq "$LINE3" ~/.bashrc; then
    echo "$LINE3" >> ~/.bashrc
    echo "✅ Added ndustria/bin to PATH in ~/.bashrc"
fi

echo "💡 Done! Please run: source ~/.bashrc"

ndustria --setup
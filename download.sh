#!/bin/bash
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate data_inconsistency
python download.py --use_ckpt

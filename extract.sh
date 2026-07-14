#!/bin/bash
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate data_inconsistency
python3 /home/levantuananh/FFT_FPT/data_inconsistency/tex_drive_pipeline.py \
  --source-folder-id 1g2uGiWJMCiMenw8_IKFga1M7iIJIaLax \
  --output-folder-id 1PqhB3lAWt5j1lilXg0Mg5ldM2U8M_1_- \
  --checkpoint-file checkpoint/tex_pipeline_checkpoint.json \
  --delete-source \
  --workers 8

import sys
import logging
import json
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))


from post_processing.merge import PostProcessingPipeline

pipeline = PostProcessingPipeline()
results = pipeline.run()
print(results)
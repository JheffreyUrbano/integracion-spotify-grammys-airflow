from pathlib import Path
import pandas as pd
from ydata_profiling import ProfileReport

def generate_profile(input_path: str, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(input_path)
    profile = ProfileReport(df, title="Data Profiling Report", minimal=True)
    profile.to_file(output_path)
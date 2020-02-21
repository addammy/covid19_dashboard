import sys
from pathlib import Path
import pickle

parent_dir = Path(__file__).resolve().parent
src_dir = parent_dir / '../src'
sys.path.insert(0, str(src_dir))

from corona.epirisk import setup_epirisk
from corona.hopkins import get_cases_as_df

cases_df = get_cases_as_df()
results = {}
for name, group in cases_df.groupby('Date'):
    print(name)
    epirisk = setup_epirisk(group)
    risks = epirisk.get_risk()
    max_30 = sorted(risks.distribution.keys(), key=risks.distribution.get, reverse=True)[:30]
    exported_cases = epirisk.get_exported_cases()
    exported_top_30 = epirisk.get_exported_case_per_target(max_30)
    results[name] = {
        'distribution': risks.distribution_df(),
        'connections': risks.connections_df(),
        'exported_cases': exported_cases.df(True),
        'exported_top_30': exported_top_30.df(True)
    }

with open('epirisk_outputs.pickle', 'wb') as outpickle:
    pickle.dump(results, outpickle)

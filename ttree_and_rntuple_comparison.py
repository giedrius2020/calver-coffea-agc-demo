# ---
# jupyter:
#   jupytext:
#     notebook_metadata_filter: all,-jupytext.text_representation.jupytext_version
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.10.13
# ---

# %% [markdown]
# ### AGC + calver coffea on coffea-casa
#
# We'll base this on a few sources:
# - https://github.com/iris-hep/analysis-grand-challenge/tree/main/analyses/cms-open-data-ttbar (AGC, of course)
# - https://github.com/alexander-held/CompHEP-2023-AGC (contains a simplified version of AGC)
# - https://github.com/nsmith-/TTGamma_LongExercise/ (credit Nick Smith for helpful examples of the new API)
# - (and if time allows, weight features: https://github.com/CoffeaTeam/coffea/blob/backports-v0.7.x/binder/accumulators.ipynb / https://coffeateam.github.io/coffea/api/coffea.analysis_tools.Weights.html#coffea.analysis_tools.Weights.partial_weight)

# %%
from pathlib import Path
import matplotlib.pyplot as plt

import awkward as ak
import numpy as np
import uproot
import traceback
from dask.distributed import Client
import skhep_testdata
import pandas as pd

import warnings

import utils
utils.plotting.set_style()

warnings.filterwarnings("ignore")

# client = Client("tls://localhost:8786")

print(f"awkward: {ak.__version__}")
print(f"uproot: {uproot.__version__}")

# %% [markdown]
# ### Produce an AGC histogram with Dask (no coffea yet)

# %%

# %% [markdown]
# Reading in the ROOT file, we can now create a Dask task graph for the calculations and plot that we want to make using `dask-awkward` and `hist.dask`

# %%
all_files = {}
events_list = []

# Some files are downloaded locally:
# all_files.append(ttbar_file)
# all_files.append("root://eospublic.cern.ch//eos/root-eos/AGC/nanoAOD/TT_TuneCUETP8M1_13TeV-powheg-pythia8/cmsopendata2015_ttbar_19981_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext4-v1_80000_0007.root") # ttbar remote 533M size
# all_files.append("root://eospublic.cern.ch//eos/root-eos/AGC/rntuple/nanoAOD/TT_TuneCUETP8M1_13TeV-amcatnlo-pythia8/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root") # RNTuple remote
# all_files.append("root://eospublic.cern.ch//eos/root-eos/AGC/nanoAOD/TT_TuneCUETP8M1_13TeV-amcatnlo-pythia8/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root") # TTree remote
# all_files.append("/home/cms-jovyan/my_root_files/rntuple_v1.root") # RNTuple, local, with our own converter v4 
# all_files.append("/home/cms-jovyan/my_root_files/rntuple_v2.root") # RNTuple, local, with our own converter v4 
# all_files.append("/home/cms-jovyan/my_root_files/rntuple_v3.root") # RNTuple, local, with our own converter v4 

# all_files.append("/home/cms-jovyan/my_root_files/rntuple_v4.root") # RNTuple, local, with our own converter v4 


# all_files.append("/home/cms-jovyan/my_root_files/ttree/cmsopendata2015_ttbar_19981_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext4-v1_80000_0007.root") # TTree ttbar original
all_files["TT"] = "/home/cms-jovyan/my_root_files/ttree/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root" # TTree local
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root"  # RNTuple local
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_v1.root" # RNTuple, local, with our own converter v4 
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_v2.root" # RNTuple, local, with our own converter v4 
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_v3.root" # RNTuple, local, with our own converter v4 
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_v4.root" # RNTuple, local, with our own converter v4 
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_0903_v5.root" # RNTuple, local, with our own converter v5
all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_v6_632_0909.root" # RNTuple, local, with our own converter v5
# all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple_v7_6_0909.root" # RNTuple, local, with our own converter v5





def load_files_with_uproot(files):
    for fl in files.values():
        with uproot.open(fl) as f:
            events = f["Events"]
            events_list.append(events)
            print("File was loaded with uproot, event count: ", len(events.keys()))
            
            # NOTE: to access array: # events.arrays(["Electron_pt"])["Electron_pt"]
        
def load_files_with_coffea(files):
    for fl in files:
        events = NanoEventsFactory.from_root({fl: "Events"}, schemaclass=NanoAODSchema).events()
        events_list.append(events)
        print("File was loaded with coffea, fields count: ", len(events.fields))
        
load_files_with_uproot(all_files)

# load_files_with_coffea(all_files)





# %%
# # Various available properties:
# print("Name: ", events.name)
# print("header: ", events.header)
# print("footer: ", events.footer)
# print("num_entries: ", events.num_entries)
# print("len of field_names: ", len(events.field_names))
# print("keys: ", len(events.keys()))
# print(" field_names: ", events.fields)
# print("column_records: ", events.column_records[:10])
# print("keys: ", events.keys()[:10])
# print("_column_records_dict: ", events._column_records_dict)
# print("_related_ids: ", events._related_ids)
# print("page_list_envelopes: ", events.page_list_envelopes)

# %%
import timeit
events_dict = {}

def format_test_results(times):
    df = pd.DataFrame(times, columns =['data_type', 'func_name', 'time(s)'])
    df = df.sort_values(by=['func_name'])
    df['time(s)'] = df['time(s)'].round(4)
    df['func_name'] = df['func_name'].str.replace('_', ' ', regex=False)
    
    return df

print("Starting to timeit on various functions: ")

def load_file(data_type, file):
    with uproot.open(file) as f:
        events = f["Events"]
        events_dict[data_type] = events

def load_arrays_for_each_key(events):
    for key in events.keys():
        events.arrays(filter_name=[key])[key]
        
def load_all_arrays(events):
    events.arrays()
    
def load_all_arrays_while_using_filter_name(events):
    chosen_keys = events.keys()
    events.arrays(filter_name=chosen_keys)[chosen_keys]

def load_array_while_using_filter_name(events):
    key = "nGenVisTau"
    events.arrays(filter_name=[key])[key]
    
    
def load_10_arrays_while_using_filter_name(events):
    chosen_keys = [ "nGenVisTau",
                    "GenVisTau_eta",
                    "GenVisTau_mass",
                    "GenVisTau_phi",
                    "GenVisTau_pt",
                    "GenVisTau_charge",
                    "GenVisTau_genPartIdxMother",
                    "GenVisTau_status",
                    "genWeight",
                    "LHEWeight_originalXWGTUP"]
    
    events.arrays(filter_name=chosen_keys)[chosen_keys]
        
def start_all_performance_tests():
    times = []
    
    for data_type, file in all_files.items():
        time_taken = timeit.timeit(lambda: load_file(data_type, file), number=1)
        times.append((data_type, "load_file", time_taken))

        time_taken = timeit.timeit(lambda: load_arrays_for_each_key(events_dict[data_type]), number=1)
        times.append((data_type, "load_arrays_for_each_key", time_taken))
        
        time_taken = timeit.timeit(lambda: load_all_arrays(events_dict[data_type]), number=1)
        times.append((data_type, "load_all_arrays", time_taken))
        
        time_taken = timeit.timeit(lambda: load_all_arrays_while_using_filter_name(events_dict[data_type]), number=1)
        times.append((data_type, "load_all_arrays_while_using_filter_name", time_taken))
        
        time_taken = timeit.timeit(lambda: load_10_arrays_while_using_filter_name(events_dict[data_type]), number=1)
        times.append((data_type, "load_10_arrays_while_using_filter_name", time_taken))
        
        time_taken = timeit.timeit(lambda: load_array_while_using_filter_name(events_dict[data_type]), number=1)
        times.append((data_type, "load_array_while_using_filter_name", time_taken))

    
    return format_test_results(times)

results = start_all_performance_tests()


# %%
print(results.to_string(index=False))


# %%
# This cell compares keys for TTree and RNTuple. For each matching key, it compares all array values. At the end, comparison statistics are printed.
def compare_key_lists(ls1, ls2):
    match_count = 0
    mismatch_count = 0
    
    ak_match_count = 0
    ak_mismatch_count = 0
    ak_error_count = 0
    
    count_of_all_tt_elements = 0
    count_of_all_rn_elements = 0
    
    for i in range(len(ls1)):
        if keys_tt[i] == keys_rn[i]:
            key = keys_tt[i]
            match_count+=1

            arrays_tt = events_tt.arrays([key])[key]
            arrays_rn = events_rn.arrays([key])[key]
            
            el_count_tt = len(ak.ravel(arrays_tt))
            el_count_rn = len(ak.ravel(arrays_rn))
            

            # Check if arrays are equal:
            try:                
                # Custom function to compare NaN-aware equality
                def nan_equal(x, y):
                    if isinstance(x, (list, ak.Array)) and isinstance(y, (list, ak.Array)):
                        return all(nan_equal(a, b) for a, b in zip(x, y))
                    return (x == y) or (np.isnan(x) and np.isnan(y))
                # Check if the lengths of the outermost arrays are equal
                assert len(arrays_tt) == len(arrays_rn)
                
                # Compare the arrays using the custom function
                are_equal = nan_equal(arrays_tt.tolist(), arrays_rn.tolist())
                
                if are_equal:
                    ak_match_count += 1
                    print(f"[{key}]", "ak arrays are equal")
                elif not are_equal:
                    count_of_all_tt_elements+=el_count_tt
                    count_of_all_rn_elements+=el_count_rn
                    ak_mismatch_count += 1
                    print(f"[{key}]", "ak comparison MISMATCH")
                    print("tt: ", arrays_tt, f"Type: {ak.type(arrays_tt)}. Count of elements: {el_count_tt}")
                    print("rn: ", arrays_rn, f"Type: {ak.type(arrays_rn)}. Count of elements: {el_count_rn}")
                
            except:
                count_of_all_tt_elements+=el_count_tt
                count_of_all_rn_elements+=el_count_rn
                ak_error_count += 1
                print(f"[{key}]", "ak comparison ERROR")
                print("tt: ", arrays_tt, f"Type: {ak.type(arrays_tt)}. Count of elements: {el_count_tt}")
                print("rn: ", arrays_rn, f"Type: {ak.type(arrays_rn)}. Count of elements: {el_count_rn}")
        else:
            mismatch_count+=1
            # print("Mismatch: ", keys_tt[i], "---", keys_rn[i])
    print(f"Keys comparison statistics: matched count: {match_count}; mismatch count: {mismatch_count}")
    print(f"ak array comparison statistics: matched count: {ak_match_count}; mismatch count: {ak_mismatch_count}; errors: {ak_error_count}")


    
events_tt = events_list[0]
events_rn = events_list[1]

# # Must be sorted, because otherwise the order is different.
keys_tt = sorted(events_tt.keys(), key=str.lower)
keys_rn = sorted(events_rn._keys, key=str.lower)

print(f"TTree keys length: {len(keys_tt)}. RNTuple keys length: {len(keys_rn)}")
# compare_key_lists(keys_tt, keys_rn)

# %%
def collect_breaking_points(key):
    cluster_starts = [md.num_first_entry for md in events_rn.cluster_summaries][1:] # Skip first, because it is 0.
    print("Starts of clusters: ", cluster_starts)

    step = 4
    for cl_start in cluster_starts:
        for i in range (cl_start-19, cl_start+19, step):
            strt = i
            end = i + step
            arr_tt = events_tt.arrays(filter_name=[key], entry_start=strt, entry_stop=end)[key]
            arr_rn = events_rn.arrays(filter_name=[key], entry_start=strt, entry_stop=end)[key]
            
            try:
                 # Custom function to compare NaN-aware equality
                def nan_equal(x, y):
                    if isinstance(x, (list, ak.Array)) and isinstance(y, (list, ak.Array)):
                        return all(nan_equal(a, b) for a, b in zip(x, y))
                    return (x == y) or (np.isnan(x) and np.isnan(y))
                # Check if the lengths of the outermost arrays are equal
                assert len(arr_tt) == len(arr_rn)
                
                # Compare the arrays using the custom function
                are_equal = nan_equal(arr_tt.tolist(), arr_rn.tolist())
                assert(are_equal)
                # print("EQUAL:")
                # print(f"TT array: {ak.to_list(arr_tt)}")
                # print(f"RN array: {ak.to_list(arr_rn)}")
            except Exception as e:
                print(f"TT array: {arr_tt}")
                print(f"RN array: {arr_rn}")
                print("Index: ", i, f". Failure limits: {(strt, end)}")
                print("")

key = "Electron_hoe"
# collect_breaking_points(key)
print(uproot.const.rntuple_col_type_to_num_dict["splitindex32"])
# print("Finished cell.")


# %%
# print(events_tt.keys(filter_name=["run", "Electron_hoe"]))
# print(events_rn.keys(filter_name=["run", "Electron_hoe"]))
# print(events_rn.keys())


# %%
def compare_array_region(key, events_tt, events_rn):
    cluster_starts = [md.num_first_entry for md in events_rn.cluster_summaries][1:] # Skip first, because it is 0.
    print("Starts of clusters: ", cluster_starts)
    
    strt = 44431
    end = 44450

    arr_tt = events_tt.arrays(filter_name=[key], entry_start=strt, entry_stop=end)[key]
    arr_rn = events_rn.arrays(filter_name=[key], entry_start=strt, entry_stop=end)[key]

    try:
        # Custom function to compare NaN-aware equality
        def nan_equal(x, y):
            if isinstance(x, (list, ak.Array)) and isinstance(y, (list, ak.Array)):
                return all(nan_equal(a, b) for a, b in zip(x, y))
            return (x == y) or (np.isnan(x) and np.isnan(y))
        # Check if the lengths of the outermost arrays are equal
        assert len(arr_tt) == len(arr_rn)
        # Compare the arrays using the custom function
        comparison_result = nan_equal(arr_tt.tolist(), arr_rn.tolist())
        # Final assertion
        assert comparison_result
    except Exception as e:
        print(f"TT array: {arr_tt}")
        print(f"RN array: {arr_rn}")
        print(f"Failure limits: {(strt, end)}")
        print("")

    print("TT:", arr_tt)
    print("RN:", arr_rn)
    
# key = "HTXS_Higgs_y"
# key = "SV_pAngle"
compare_array_region(key, events_tt, events_rn)
# print("Finished cell")


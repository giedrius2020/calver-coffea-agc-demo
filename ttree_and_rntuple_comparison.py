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
# ### TTree and RNTuple loading comparison while using uproot
#
# Sources:
# - 
# - 

# %%
import awkward as ak
import numpy as np
import uproot
import pandas as pd


print(f"awkward: {ak.__version__}")
print(f"uproot: {uproot.__version__}")

# %% [markdown]
# ### File loading

# %%
all_files = {}
events_list = []

## Remote files:
# all_files.append("root://eospublic.cern.ch//eos/root-eos/AGC/rntuple/nanoAOD/TT_TuneCUETP8M1_13TeV-amcatnlo-pythia8/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root") # RNTuple remote
# all_files.append("root://eospublic.cern.ch//eos/root-eos/AGC/nanoAOD/TT_TuneCUETP8M1_13TeV-amcatnlo-pythia8/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root") # TTree remote

# Files downloaded locally:
all_files["TT"] = "/home/cms-jovyan/my_root_files/ttree/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root" # TTree local
all_files["RN"] = "/home/cms-jovyan/my_root_files/rntuple/cmsopendata2015_ttbar_19978_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext1-v1_60000_0004.root"  # RNTuple local


# all_files["632"] = "/home/cms-jovyan/my_root_files/rntuple_v6_632_0909.root" # RNTuple, ROOT_632 (works)
# all_files["6x"] = "/home/cms-jovyan/my_root_files/rntuple_v7_6_0909.root" # RNTuple, ROOT_6_X (does not work)


def load_files_with_uproot(files):
    for fl in files.values():
        with uproot.open(fl) as f:
            events = f["Events"]
            events_list.append(events)
            # print("File was loaded with uproot, event count: ", len(events.keys()))
            
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

# %% [markdown]
# ### timeit tests:

# %%
import timeit
events_dict = {}

def format_test_results(times):
    df = pd.DataFrame(times, columns =['data_type', 'func_name', 'time(s)'])
    df = df.sort_values(by=['func_name'])
    df['time(s)'] = df['time(s)'].round(4)
    df['func_name'] = df['func_name'].str.replace('_', ' ', regex=False)
    
    return df


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
    
    
def load_24_arrays_while_using_filter_name(events):
    chosen_keys = [
        "GenPart_pt", "GenPart_eta", "GenPart_phi", "CorrT1METJet_phi",
        "GenJet_pt", "CorrT1METJet_eta", "SoftActivityJet_pt",
        "Jet_eta", "Jet_phi", "SoftActivityJet_eta", "SoftActivityJet_phi", 
        "CorrT1METJet_rawPt", "Jet_btagDeepFlavB", "GenJet_eta", 
        "GenPart_mass", "GenJet_phi",
        "Jet_puIdDisc", "CorrT1METJet_muonSubtrFactor", "Jet_btagDeepFlavCvL",
        "Jet_btagDeepFlavQG", "Jet_mass", "Jet_pt", "GenPart_pdgId",
        "Jet_btagDeepFlavCvB", "Jet_cRegCorr"
        ]
    
    events.arrays(filter_name=chosen_keys)[chosen_keys]
        
def start_all_performance_tests():
    print("Starting to timeit on various functions: ")
    times = []
    
    for data_type, file in all_files.items():
        time_taken = timeit.timeit(lambda: load_file(data_type, file), number=1)
        times.append((data_type, "load_file", time_taken))

#         time_taken = timeit.timeit(lambda: load_arrays_for_each_key(events_dict[data_type]), number=1)
#         times.append((data_type, "load_arrays_for_each_key", time_taken))
        
#         time_taken = timeit.timeit(lambda: load_all_arrays(events_dict[data_type]), number=1)
#         times.append((data_type, "load_all_arrays", time_taken))
        
#         time_taken = timeit.timeit(lambda: load_all_arrays_while_using_filter_name(events_dict[data_type]), number=1)
#         times.append((data_type, "load_all_arrays_while_using_filter_name", time_taken))
        
#         time_taken = timeit.timeit(lambda: load_24_arrays_while_using_filter_name(events_dict[data_type]), number=1)
#         times.append((data_type, "load_24_arrays_while_using_filter_name", time_taken))
        
        time_taken = timeit.timeit(lambda: load_array_while_using_filter_name(events_dict[data_type]), number=1)
        times.append((data_type, "load_array_while_using_filter_name", time_taken))

    
    return format_test_results(times)


results = start_all_performance_tests()
print(results.to_string(index=False))

# %%
# print(results.to_string(index=False))

# Pivot the DataFrame
df_pivot = results.pivot(index='func_name', columns='data_type', values='time(s)')

# Clean up the columns and reset index if needed
df_pivot.columns.name = None  # Remove the name of the columns
df_pivot = df_pivot.reset_index()  # Reset the index if you want a cleaner look

# Output the pivoted DataFrame
print(df_pivot.to_markdown(index=False))


# %%
# This cell compares data between TTree and RNTuple for each key array, ensuring that RNTuple does not have corrupted data:
def compare_all_arrays(events_1, events_2, keys):
    ak_match_count = 0
    ak_mismatch_count = 0
    ak_error_count = 0
        
    for key in keys:
        arrays_1 = events_1.arrays([key])[key]
        arrays_2 = events_2.arrays([key])[key]

        # Check if arrays are equal:
        try:                
            # Custom function to compare NaN-aware equality
            def nan_equal(x, y):
                if isinstance(x, (list, ak.Array)) and isinstance(y, (list, ak.Array)):
                    return all(nan_equal(a, b) for a, b in zip(x, y))
                return (x == y) or (np.isnan(x) and np.isnan(y))
            # Check if the lengths of the outermost arrays are equal
            assert len(arrays_1) == len(arrays_2)

            # Compare the arrays using the custom function
            are_equal = nan_equal(arrays_1.tolist(), arrays_2.tolist())

            if are_equal:
                ak_match_count += 1
                print(f"[{key}]", "ak arrays are equal")
            elif not are_equal:
                ak_mismatch_count += 1
                print(f"[{key}]", "ak comparison MISMATCH")
                print("tt: ", arrays_1, f"Type: {ak.type(arrays_1)}.")
                print("rn: ", arrays_2, f"Type: {ak.type(arrays_2)}.")

        except:
            ak_error_count += 1
            print(f"[{key}]", "ak comparison ERROR")
            print("tt: ", arrays_1, f"Type: {ak.type(arrays_1)}")
            print("rn: ", arrays_2, f"Type: {ak.type(arrays_2)}")

    print(f"ak array comparison statistics: matched count: {ak_match_count}; mismatch count: {ak_mismatch_count}; errors: {ak_error_count}")
    
events_tt = events_list[0]
events_rn = events_list[1]

keys = [
        "GenPart_pt", "GenPart_eta", "GenPart_phi", "CorrT1METJet_phi",
        "GenJet_pt", "CorrT1METJet_eta", "SoftActivityJet_pt",
        "Jet_eta", "Jet_phi", "SoftActivityJet_eta", "SoftActivityJet_phi", 
        "CorrT1METJet_rawPt", "Jet_btagDeepFlavB", "GenJet_eta", 
        "GenPart_mass", "GenJet_phi",
        "Jet_puIdDisc", "CorrT1METJet_muonSubtrFactor", "Jet_btagDeepFlavCvL",
        "Jet_btagDeepFlavQG", "Jet_mass", "Jet_pt", "GenPart_pdgId",
        "Jet_btagDeepFlavCvB", "Jet_cRegCorr"
        ]

compare_all_arrays(events_tt, events_rn, keys)

    


# %%
# Comparing only certain regions of arrays:
def compare_array_region(key, events_tt, events_rn, strt, end):
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
        return True
    except Exception as e:
        print(f"TT array: {arr_tt}")
        print(f"RN array: {arr_rn}")
        print(f"Failure limits: {(strt, end)}")
        print("")
        return False

# Collect all regions near cluster edges, where data does not match:
def collect_breaking_points(key):
    cluster_starts = [md.num_first_entry for md in events_rn.cluster_summaries][1:] # Skip first, because it is 0.
    print("Starts of clusters: ", cluster_starts)

    step = 4
    for cl_start in cluster_starts:
        for i in range (cl_start-9, cl_start+9, step):
            strt = i
            end = i + step
            result = compare_array_region(key, events_tt, events_rn, strt, end)
            print(f"Range: ({strt},{end}). Match result: {result}")

key = "Electron_hoe"
collect_breaking_points(key)
print("Finished cell.")


# %%
cluster_starts = [md.num_first_entry for md in events_632.cluster_summaries][1:] # Skip first, because it is 0.
print("Starts of clusters: ", cluster_starts)
events_632 = events_list[0]
events_6x = events_list[1]
print("Keys: ", events_6x.keys())
print("Keys: ", events_632.keys())






# %%

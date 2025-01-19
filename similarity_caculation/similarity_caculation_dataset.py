import pandas as pd
from difflib import SequenceMatcher
from collections import defaultdict

# Load data
file_path = 'huggingface_datasets.xlsx'
file_path100 = 'huggingface_datasets_top100.xlsx'
df = pd.read_excel(file_path)
df100 = pd.read_excel(file_path100)

# Convert all names to string type
df['Author'] = df['Author'].astype(str)
df['Dataset Name'] = df['Dataset Name'].astype(str)
df100['Author'] = df100['Author'].astype(str)
df100['Dataset Name'] = df100['Dataset Name'].astype(str)

# Function to find similar names based on SequenceMatcher
def find_similar_names(names, other_column):
    similar_names = defaultdict(list)
    for name1 in names:
        if len(name1) > 3 and len(name1) <= 6:
            threshold = 0.9  # 90% similarity for names of length 4-6
        elif len(name1) >= 7:
            threshold = 0.8  # 80% similarity for names of length 7 or more
        else:
            threshold = 0.0   # No threshold for names of length 3 or less

        for name2 in other_column:
            if isinstance(name1, str) and isinstance(name2, str):
                if name1 != name2 and SequenceMatcher(None, name1.lower(), name2.lower()).ratio() >= threshold:
                    similar_names[name1].append(name2)
                    similar_names[name2].append(name1)
    return similar_names

# Find similar organization names
organization_names = df['Author'].unique()
organization_names100 = df100['Author'].unique()
print(f"Unique Author names: {organization_names}")
similar_organizations = find_similar_names(organization_names100, organization_names)
print(f"Similar Author: {similar_organizations}")

# Function to find similar model names, skipping datasets from the same organization
def find_similar_model_names(names100, other_column100, names, other_column):
    similar_names = defaultdict(list)
    organization_for_dataset1 = defaultdict(list)
    organization_for_dataset2 = defaultdict(list)

    for (name1, other1) in zip(names100, other_column100):
        if len(name1) > 3 and len(name1) <= 6:
            threshold = 0.9
        elif len(name1) >= 7:
            threshold = 0.8
        else:
            threshold = 0.0
            
        for (name2, other) in zip(names, other_column):
            for other2 in other:
                if other1 != other2 and SequenceMatcher(None, name1.lower(), name2.lower()).ratio() >= threshold:
                    similar_names[name1].append(name2)
                    similar_names[name2].append(name1)
                    organization_for_dataset1[name1].append(other1)
                    organization_for_dataset1[other1].append(name1)
                    organization_for_dataset2[name2].append(other2)
                    organization_for_dataset2[other2].append(name2)
    return similar_names, organization_for_dataset, organization_for_dataset2

dataset_names = df['Dataset Name'].unique()
dataset_names100 = df100['Dataset Name'].unique()
organization_for_dataset100 = df100.set_index('Dataset Name')['Author'].to_dict()
organization_for_dataset = df.groupby('Dataset Name')['Author'].apply(list).to_dict()

similar_dataset, organization_for_dataset1, organization_for_dataset2 = find_similar_model_names(
    dataset_names100,
    [organization_for_dataset100[dataset] for dataset in dataset_names100],
    dataset_names,
    [organization_for_dataset[dataset] for dataset in dataset_names]
)
print(f"Similar dataset: {similar_dataset}")

# Convert results to DataFrame for export
def dict_to_df(similar_dict):
    rows = []
    for key, values in similar_dict.items():
        for value in values:
            rows.append((key, value))
    return pd.DataFrame(rows, columns=['Name1', 'Name2'])

def dict_to_df_dataset(similar_dict, organization_dict1, organization_dict2):
    rows = []
    for key, values in similar_dict.items():
        for value in values:
            org1 = organization_dict1.get(key)
            org2 = organization_dict2.get(value)
            rows.append((key, value, org1, org2))
    return pd.DataFrame(rows, columns=['Name1', 'Name2', 'Author1', 'Author2'])


similar_dataset_df = dict_to_df_dataset(similar_dataset, organization_for_dataset, organization_for_dataset)

# Save results to Excel files

similar_dataset_df.to_excel('similar_datafinal.xlsx', index=False)
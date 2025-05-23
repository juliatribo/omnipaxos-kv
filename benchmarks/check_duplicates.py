import pandas as pd

df1 = pd.read_json('../build_scripts/logs/key-val-s1.json')
duplicates = df1[df1.duplicated([1], keep=False)]
duplicates = duplicates.sort_values(by=[1])
print(f"Number of duplicates: {len(duplicates)}")

#check if different servers have the same database as df1
df2 = pd.read_json('../build_scripts/logs/key-val-s2.json')

for i in range(2, 8):
    path = f'../build_scripts/logs/key-val-s{i}.json'
    df = pd.read_json(path)
    
    if df1.equals(df):
        print(f"Server {i}: EXACTLY the same as server 1 ✅")
    else:
        print(f"Server {i}: DIFFERENT from server 1 ❌")
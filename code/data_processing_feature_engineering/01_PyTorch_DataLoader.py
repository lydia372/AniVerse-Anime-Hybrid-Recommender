import torch
import pandas as pd
from torch.utils.data import Dataset, DataLoader

# 1. Load the split data from Caleb's new splits
folder_path = '/content/drive/MyDrive/BT4222_Group11' 
train_df = pd.read_csv(f'{folder_path}/new_splits/train_adjusted.csv')
val_df = pd.read_csv(f'{folder_path}/new_splits/val_adjusted.csv')
test_df = pd.read_csv(f'{folder_path}/new_splits/test_adjusted.csv')

# 2. The Dataset Class (The Blueprint)
class AnimeInteractionDataset(Dataset):
    def __init__(self, dataframe):
        self.users = torch.tensor(dataframe['user_idx'].values, dtype=torch.long)
        self.animes = torch.tensor(dataframe['anime_idx'].values, dtype=torch.long)
        self.ratings = torch.tensor(dataframe['adjusted_score'].values, dtype=torch.float32)

    def __len__(self):
        return len(self.users)

    def __getitem__(self, idx):
        return self.users[idx], self.animes[idx], self.ratings[idx]

# 3. Create the DataLoaders
BATCH_SIZE = 1024 
train_loader = DataLoader(AnimeInteractionDataset(train_df), batch_size=BATCH_SIZE, shuffle=True)
val_loader = DataLoader(AnimeInteractionDataset(val_df), batch_size=BATCH_SIZE, shuffle=False)
test_loader = DataLoader(AnimeInteractionDataset(test_df), batch_size=BATCH_SIZE, shuffle=False)
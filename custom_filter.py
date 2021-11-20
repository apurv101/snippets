
import numpy as np
import pandas as pd

def get_roof(ins,outs):

    classification = ins['Classification']
    hag = ins['HeightAboveGround']
    e0 = ins['Eigenvalue0']
    nor = ins['NumberOfReturns']
    rn = ins['ReturnNumber']
    copl = ins['Coplanar']
    Z = np.where((hag > 1) & (e0 <= 0.05) & (nor == rn) & (copl == 1), np.int32(1), np.int32(0))
    outs['roof'] = Z
    return True



def get_trees(ins,outs):
    classification = ins['Classification']
    hag = ins['HeightAboveGround']
    e0 = ins['Eigenvalue0']
    nor = ins['NumberOfReturns']
    rn = ins['ReturnNumber']
    copl = ins['Coplanar']
    Z = np.where((e0 > 0.005) & (nor - rn >= 1) & (copl == 0), np.int32(1), np.int32(0))
    outs['tree'] = Z
    return True


def remove_ouliers_in_roof(ins,outs):
    df = pd.DataFrame(ins)
    df_roof = df[df['roof'] == 1]
    cluster_density = df_roof['ClusterID'].value_counts()/df_roof['ClusterID'].shape[0]
    cluster_density.drop([-1], inplace=True)
    fcd = cluster_density.where(cluster_density > 0.1).dropna()
    df['outliers'] = np.int32(0)
    df.loc[df['roof'] == 1 & ~df['ClusterID'].isin(fcd.keys()), 'outliers'] = np.int32(1)
    df.loc[(df['roof'] == 1) & (df['Classification'] == 7), 'outliers'] = np.int32(1)
#     df.loc[df['roof'] == 0, 'finalroof'] = np.int32(1)
#     df.loc[df['Classification'] != 7, 'finalroof'] = np.int32(1)
#     df.loc[df['roof'] == 0 | ~df['ClusterID'].isin(fcd.keys()), 'finalroof'] = np.int32(0)
    df['finalroof'] = np.int32(0)
    df.loc[(df['roof'] == 1) & (df['outliers'] == 0), 'finalroof'] = np.int32(1)
    outs['outliers'] = df['outliers'].to_numpy()
    outs['finalroof'] = df['finalroof'].to_numpy()
    return True
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

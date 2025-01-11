import pandas as pd
from sklearn.preprocessing import LabelEncoder,MinMaxScaler
import os

def Extract(f):
    data= pd.read_csv("raw_data.csv")
    return data
def trans(data):
    #fillna
    data["Age"].fillna(data["Age"].mean(),inplace=True)
    data["Monthly Recharge Cost (INR)"].fillna(data["Monthly Recharge Cost (INR)"].mode()[0],inplace=True)
    
    #Categorical data
    la_en=LabelEncoder()
    data['Gender']=la_en.fit_transform(data["Gender"])
    data['Location']=la_en.fit_transform(data["Location"])
    data['OS']=la_en.fit_transform(data["OS"])
    
    #normalize
    scale=MinMaxScaler()
    data[['Age','Monthly Recharge Cost (INR)']]=scale.fit_transform(data[['Age','Monthly Recharge Cost (INR)']])
    
    return data
def load(data,pro_ed_file):
    
    if os.path.exists(pro_ed_file):
        os.remove(pro_ed_file)
        print(f"Old file '{pro_ed_file}' deleted.")
        
    data.to_csv(pro_ed_file,index=False)
    print(f"processed data saved to {pro_ed_file}")
    

#Automate the pipline

if __name__ =="__main__":
    f="raw_data.csv"
    pro_ed_file="processed_data.csv"
    
    extract_data=Extract(f)
    trans_data=trans(extract_data)
    load(trans_data,pro_ed_file)
    
    print("ETL piplined executed success")
#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import plotly.io as pio
import plotly.graph_objects as go
import math
import pyodbc
from tqdm import tqdm

class QpKpiProcess:
    def __init__(self, server, database, username, password, driver='{ODBC Driver 17 for SQL server}'):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.conn = None  # Placeholder for the database connection

    def connect_to_database(self):
        conn_string = f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
        self.conn = pyodbc.connect(conn_string)
        print("Connected to database successfully.")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Connection closed.")

    def read_table_to_df(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.conn)

    def plot(self, df, breach_percentage, title, start_date, end_date, color):
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=breach_percentage,
            number={'suffix': "%"},
            domain={'x': [0, 1], 'y': [0, 1]},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                'bar': {'color': color},
                'bgcolor': "white",
                'borderwidth': 2,
            }
        ))
        fig.update_layout(
            title_text=title,
            title_x=0.5,
            annotations=[
                dict(
                    text='Number of Active projects: '+str(df[['project_code','sprint_name']].drop_duplicates().shape[0]) +
                    '<br>' + 'Number of projects that breached LSL: '+str(df[df['Verdict'] == 'red'][['project_code','sprint_name']].drop_duplicates().shape[0]) +
                    '<br>' + 'Start Date - ' + str(start_date) +
                    '<br>' + 'End Date - ' + str(end_date) +
                    '<br>' + 'Duration - ' + str(end_date-start_date),
                    x=1.05, y=1.1,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=14, color="black")
                )
            ]
        )
        fig.show()

    def RSI(self, start_date, end_date, df, refer_data):
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        df = df[(df['sprint_start_date'] >= start_date) & (df['sprint_end_date'] <= end_date)]
        df = df[df['status_waiver'] == 'Open']
        RSI_data = df.groupby(['project_code', 'sprint_name','sprint_start_date', 'sprint_end_date','status_waiver'], as_index=False)[['ttl_stry_pts_add_sprnt', 'ttl_stry_pts_del_sprnt', 'ttl_stry_pts_mod_sprnt','ttl_story_pts_planned']].sum()
        RSI_data['RSI'] = (1-((RSI_data['ttl_stry_pts_add_sprnt'] + RSI_data['ttl_stry_pts_del_sprnt'] + RSI_data['ttl_stry_pts_mod_sprnt'])/ RSI_data['ttl_story_pts_planned'])) * 100
        lsl = refer_data['requirement_stability_index_rsi_lsl'][0]
        usl = refer_data['requirement_stability_index_rsi_usl'][0]
        RSI_data['Verdict'] = RSI_data['RSI'].apply(lambda x: 'green' if x >= float(lsl) else 'red')
        breach_percentage = (RSI_data[RSI_data['Verdict'] == 'red'][['project_code','sprint_name']].drop_duplicates().shape[0]/RSI_data[['project_code','sprint_name']].drop_duplicates().shape[0])*100
        color = 'green' if breach_percentage <= 25 else 'blue' if breach_percentage <= 50 else 'red'
        self.plot(RSI_data, breach_percentage, 'Scope Creep to Hurt Profitability', start_date, end_date, color)
        display(RSI_data)
        return RSI_data, breach_percentage

    def DSV(self, start_date, end_date, df, refer_data):
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        df = df[(df['sprint_start_date'] >= start_date) & (df['sprint_end_date'] <= end_date)]
        df = df[df['status_waiver'] == 'Open']
        DSV_data = df.groupby(['project_code', 'sprint_name','sprint_start_date', 'sprint_end_date','status_waiver'], as_index=False)[['ttl_story_pts_actual', 'ttl_story_pts_planned']].sum()
        DSV_data['DSV'] = ((DSV_data['ttl_story_pts_actual'] - DSV_data['ttl_story_pts_planned'])/DSV_data['ttl_story_pts_planned'])*100
        lsl = refer_data['delivery_scope_variance_lsl'][0]
        usl = refer_data['delivery_scope_variance_usl'][0]
        DSV_data['Verdict'] = DSV_data['DSV'].apply(lambda x: 'green' if x >= float(lsl) else 'red')
        breach_percentage = (DSV_data[DSV_data['Verdict'] == 'red'][['project_code','sprint_name']].drop_duplicates().shape[0]/DSV_data[['project_code','sprint_name']].drop_duplicates().shape[0])*100
        color = 'green' if breach_percentage <= 25 else 'blue' if breach_percentage <= 50 else 'red'
        self.plot(DSV_data, breach_percentage, 'Rework to Hurt Profitability', start_date, end_date, color)
        display(DSV_data)
        return DSV_data, breach_percentage

    def ECU(self, start_date, end_date, df, refer_data):
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        df = df[(df['sprint_start_date'] >= start_date) & (df['sprint_end_date'] <= end_date)]
        df = df[df['status_waiver'] == 'Open']
        ECU_data = df.groupby(['project_code', 'sprint_name','sprint_start_date', 'sprint_end_date','status_waiver'], as_index=False)[['engg_team_eff_actual', 'engg_team_hrs']].sum()
        ECU_data['ECU'] = (ECU_data['engg_team_eff_actual']/ECU_data['engg_team_hrs'])*100
        lsl = refer_data['engg_capacity_utilization_lsl'][0]
        usl = refer_data['engg_capacity_utilization_usl'][0]
        ECU_data['Verdict'] = ECU_data['ECU'].apply(lambda x: 'green' if x >= float(lsl) else 'red')
        breach_percentage = (ECU_data[ECU_data['Verdict'] == 'red'][['project_code','sprint_name']].drop_duplicates().shape[0]/ECU_data[['project_code','sprint_name']].drop_duplicates().shape[0])*100
        color = 'green' if breach_percentage <= 25 else 'blue' if breach_percentage <= 50 else 'red'
        self.plot(ECU_data, breach_percentage, 'Utilization to Hurt Profitability', start_date, end_date, color)
        display(ECU_data)
        return ECU_data, breach_percentage


    def append_data(self,df1, df2, df3, col_name, value):
        # Specify the columns you want to append from df2 to df1
        columns_to_append = ['project_code', 'sprint_name', 'sprint_start_date', 'sprint_end_date', 'project_status', 'customer_account_name','project_name']
        
        df2_filtered = df2[df2['Verdict'] == 'red']
        # Renaming a single column
        df2_filtered.rename(columns={'status_waiver': 'project_status'}, inplace=True)
    
        
        df2_subset = df2_filtered.merge(df3[['customer_account_name','project_code','project_name']], on='project_code', how='left' )
        df2_subset1 = df2_subset[columns_to_append]
        
        # Reindex df2_subset to match the columns in df1, filling missing columns with NaN
        df2_subset_reindexed = df2_subset1.reindex(columns=df1.columns)
        
        df2_subset_reindexed[col_name] = value
    
        # Append df2_subset_reindexed to df1
        result_df = pd.concat([df1, df2_subset_reindexed], ignore_index=True)
        return result_df

    # Function to insert or update DataFrame into SQL table
    def insert_or_update_table(self,df,table_name):
        cursor = self.conn.cursor()
        df.fillna(0, inplace = True)
        for index, row in tqdm(df.iterrows()):
            project_code = row['project_code']
            sprint_name = row['sprint_name']
            
            # Instead of SELECT *, specify the columns you need
            existing_data = cursor.execute(f"SELECT * FROM {table_name} WHERE project_code = ? AND sprint_name = ?", (project_code, sprint_name)).fetchone()
            
            if existing_data:
                # If the combination of project_code and sprint_name exists, perform an update
                column_names = [column[0] for column in cursor.description]
                existing_data_dict = dict(zip(column_names, existing_data))
                
                # Update values where they are zero with values from the DataFrame
                for col, value in existing_data_dict.items():
                    if value == 0 and col in df.columns:
                        existing_data_dict[col] = row[col]
                
                set_columns = ', '.join([f"{col} = ?" for col in existing_data_dict.keys()])
                update_values = list(existing_data_dict.values())
                update_values.extend([project_code, sprint_name])
                
                update_query = f"UPDATE {table_name} SET {set_columns} WHERE project_code = ? AND sprint_name = ?"
                cursor.execute(update_query, tuple(update_values))
            else:
                # If the combination doesn't exist, perform an insert
                placeholders = ', '.join('?' * len(df.columns))
                columns = ', '.join(df.columns)
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row))
    
        self.conn.commit()
        cursor.close()

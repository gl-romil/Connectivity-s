#!/usr/bin/env python
# coding: utf-8

# In[1]:


from qp_kpi_process import QpKpiProcess
import pandas as pd


# In[2]:


from qp_kpi_process import QpKpiProcess

# Instantiate the class with your database details
qp_kpi_processor = QpKpiProcess(
    server='',
    database='',
    username='',
    password=''
)

# Connect to the database
qp_kpi_processor.connect_to_database()

# Perform operations, e.g., read data, calculate RSI
# Replace 'your_table_name_here' and 'your_dates_here' with actual values
qpkpi_data = qp_kpi_processor.read_table_to_df('qp_kpi_metric_table')
refer_data = qp_kpi_processor.read_table_to_df('referenced_metric_table')
ps_data = qp_kpi_processor.read_table_to_df('project_setup')
finaloutput_data= qp_kpi_processor.read_table_to_df('final_output_metric_table')

qpkpi_data['sprint_start_date']=  pd.to_datetime(qpkpi_data['sprint_start_date'], errors='coerce',format='%Y-%m-%d')
qpkpi_data['sprint_end_date']=   pd.to_datetime(qpkpi_data['sprint_end_date'], errors='coerce', format='%Y-%m-%d')

# Example operation: Calculate RSI
# You need to adjust the parameters according to your actual requirements
Calculated_RSI_data, RSI_Breach_Percentage = qp_kpi_processor.RSI('2021-11-03','2022-06-07', qpkpi_data, refer_data)

# Close the database connection when done
#qp_kpi_processor.close_connection()


# In[3]:


Calculated_DSV_data, DSV_Breach_Percentage = qp_kpi_processor.DSV('2021-11-03','2022-06-07', qpkpi_data, refer_data)


# In[4]:


Calculated_ECU_data, ECU_Breach_Percentage = qp_kpi_processor.ECU('2021-11-03','2022-06-07', qpkpi_data, refer_data)


# In[4]:


final_appended_data_RSI = qp_kpi_processor.append_data(finaloutput_data, Calculated_RSI_data, ps_data,  'requirement_stability_index', RSI_Breach_Percentage)
final_appended_data_RSI


# In[5]:


qp_kpi_processor.insert_or_update_table(final_appended_data_RSI, 'final_output_metric_table')


# In[6]:


final_appended_data_DSV = qp_kpi_processor.append_data(finaloutput_data, Calculated_DSV_data, ps_data,  'delivery_scope_variance', DSV_Breach_Percentage)
#final_appended_data_DSV


# In[7]:


qp_kpi_processor.insert_or_update_table(final_appended_data_DSV, 'final_output_metric_table')


# In[11]:


final_appended_data_ECU = qp_kpi_processor.append_data(finaloutput_data, Calculated_ECU_data, ps_data,  'engg_capacity_utilization', ECU_Breach_Percentage)
#final_appended_data_DSV


# In[12]:


qp_kpi_processor.insert_or_update_table(final_appended_data_ECU, 'final_output_metric_table')


# In[ ]:





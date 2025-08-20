# calculations.py
import streamlit as st
import pandas as pd
import state as s
def calculate_databricks_costs_for_tier(jobs_df):
    """Calculates the costs for a given list of job dictionaries."""
    if jobs_df.empty:
        cols = ["Job Name", "Runtime (hrs)", "Runs/Month", "Compute type", "Instance Type", "Nodes", "Photon","Spot", "DBU", "DBX", "EC2"]
        return pd.DataFrame(columns=cols), 0, 0

    df = jobs_df.copy()
    
    def get_rates(row):
        instance_name = s.FLAT_INSTANCE_LIST.get(row['Instance Type'])
        rate_card_row = s.FLAT_RATE_CARD.get(instance_name)
        if rate_card_row is not None:
            return rate_card_row.get('DBU/hour', 0), rate_card_row.get('Rate/hour', 0),rate_card_row.get('onDemandLinuxHr', 0)
        return 0, 0

    df[['dbu_per_hour', 'rate_per_hour', 'EC2_hr_rate']] = df.apply(lambda row: pd.Series(get_rates(row)), axis=1)
    
    # Calculate DBU units
    df['DBU_Units'] = (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    
    # Calculate costs
    df['DBU'] = df['dbu_per_hour'] * (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    df['EC2'] = df['EC2_hr_rate'] * (df["Nodes"] + 1) 
    df['DBX'] = df['rate_per_hour'] * (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    
    total_dbx_cost = df['DBX'].sum()
    total_ec2_cost = df['EC2'].sum()
    total_dbus = df['DBU'].sum()
    
    # Clean up intermediate columns before returning
    df = df.drop(columns=['dbu_per_hour', 'rate_per_hour', 'DBU_Units', 'EC2_hr_rate'], errors='ignore')

    
    return df, total_dbx_cost, total_ec2_cost,total_dbus 

def calculate_s3_cost_per_zone():
    """
    Calculates S3 cost for each individual zone, the total current cost,
    and the total 12-month projected cost.
    """
    current_costs_per_zone = {}
    projected_costs_per_zone = {} # Keep this for S3 tab display if needed, or remove if not displayed individually
    total_s3_cost = 0
    total_projected_s3_cost_12_months = 0

    # Get the S3 pricing data from the global state
    global_data = st.session_state.get('global_data', {})
    S3_PRICING = global_data.get('S3_PRICING', {})
    DEFAULT_KB_PER_RECORD_PER_COLUMN = 1.0

    if st.session_state.s3_calc_method == "Direct Storage":
        for zone, config in st.session_state.s3_direct.items():
            pricing = S3_PRICING.get(config["class"], {"storage_gb": 0})

            # Convert TB to GB for calculation
            storage_gb = config["amount"] * 1024 if config["unit"] == "TB" else config["amount"]
            
            # Current monthly cost for the zone
            storage_cost = storage_gb * pricing["storage_gb"]
            
            zone_current_cost = storage_cost 
            current_costs_per_zone[zone] = zone_current_cost
            total_s3_cost += zone_current_cost

            # Calculate 12-month projected cost for this zone (per-zone S3 growth is kept)
            monthly_growth_percent = config.get("monthly_growth_percent", 0.0)
            # if monthly_growth_percent > 0:
            #     growth_factor = 1 + (monthly_growth_percent / 100)
            #     if growth_factor != 1:
            #         zone_projected_cost = zone_current_cost * (growth_factor**12 - 1) / (growth_factor - 1)
            #     else:
            #         zone_projected_cost = zone_current_cost * 12
            # else:
            #     zone_projected_cost = zone_current_cost * 12

            # projected_costs_per_zone[zone] = zone_projected_cost
            # total_projected_s3_cost_12_months += zone_projected_cost

            # Use geometric series formula for a factor > 1
            if monthly_growth_percent > 0:
                growth_factor = 1 + (monthly_growth_percent / 100)
                
                # Quarterly cost (3 months)
                quarterly_projected_cost = zone_current_cost * ((growth_factor**3 - 1) / (growth_factor - 1))
                
                # Half-yearly cost (6 months)
                half_yearly_projected_cost = zone_current_cost * ((growth_factor**6 - 1) / (growth_factor - 1))
            else:
                # If no growth, cost is just current cost * number of months
                quarterly_projected_cost = zone_current_cost * 3
                half_yearly_projected_cost = zone_current_cost * 6
            
            # Store the new costs in the configuration dictionary
            config['quarterly_cost'] = quarterly_projected_cost
            config['half_yearly_cost'] = half_yearly_projected_cost
            
    else: # Table-Based
        standard_pricing = S3_PRICING.get("Standard", {"storage_gb": 0})
        for zone, list_of_table_configs in st.session_state.s3_table_based.items():
            zone_estimated_gb = 0
            if isinstance(list_of_table_configs, list):            
                for table_config in list_of_table_configs:
                    if isinstance(table_config, dict):
                        records = float(table_config.get("Records", 0) or 0)
                        num_columns = float(table_config.get("Columns", 0) or 0)
                        num_tables = float(table_config.get("Table", 0) or 0) # Retrieve the new 'Table' valu
                        # Calculate estimated GB: (records * num_columns * DEFAULT_KB_PER_RECORD_PER_COLUMN) / (1024 * 1024)
                        # Assuming DEFAULT_KB_PER_RECORD_PER_COLUMN is in KB
                        estimated_gb_for_table = (records * num_columns * DEFAULT_KB_PER_RECORD_PER_COLUMN) / (1024 * 1024)
                        zone_estimated_gb += estimated_gb_for_table * num_tables

            zone_current_cost = zone_estimated_gb * standard_pricing["storage_gb"]
            current_costs_per_zone[zone] = zone_current_cost
            total_s3_cost += zone_current_cost
            
            total_projected_s3_cost_12_months += zone_current_cost * 12
            projected_costs_per_zone[zone] = zone_current_cost * 12


    return current_costs_per_zone, total_s3_cost, total_projected_s3_cost_12_months

def calculate_sql_warehouse_cost():
    """Calculates total SQL Warehouse cost and DBUs from session state."""
    total_sql_cost = 0
    total_dbus = 0  
    
    global_data = st.session_state.get('global_data', {})
    sql_rates_by_type_and_instance = global_data.get('SQL_RATES_BY_TYPE_AND_INSTANCE', {})
    sql_flat_instance_list = global_data.get('SQL_FLAT_INSTANCE_LIST', {})

    for warehouse in st.session_state.sql_warehouses:
        sql_nodes = warehouse.get("SQL_nodes", 1)
        
        if warehouse.get("hours_per_day", 0) > 0 and warehouse.get("days_per_month", 0) > 0 and sql_nodes > 0:
            warehouse_type = warehouse.get("type")
            size_string = warehouse.get("size")
            
            instance_name = sql_flat_instance_list.get(size_string)
            rates = sql_rates_by_type_and_instance.get(warehouse_type, {}).get(instance_name, {})
            
            hourly_rate = rates.get('Rate/hour', 0)
            dbt_per_hr = rates.get('DBU/hour', 0)
            
            cost = hourly_rate * warehouse.get("hours_per_day", 0) * warehouse.get("days_per_month", 0) * sql_nodes
            dbus_used = dbt_per_hr * warehouse.get("hours_per_day", 0) * warehouse.get("days_per_month", 0) * sql_nodes
            
            total_sql_cost += cost
            total_dbus += dbus_used
            
    return total_sql_cost, total_dbus

def calculate_dev_costs():
    """Calculates the total cost for the development tools tab."""
    if 'dev_costs' not in st.session_state or st.session_state.dev_costs.empty:
        return 0
    
    dev_df = st.session_state.dev_costs.copy()
    global_data = st.session_state.global_data
    
    def get_rate(instance_key):
        instance_name = global_data['FLAT_INSTANCE_LIST_DEV'].get(instance_key)
        rate_info = global_data['FLAT_RATE_CARD_DEV'].get(instance_name, {})
        return rate_info.get('Rate/hour', 0.0)
    
    driver_rate = dev_df['Driver type'].apply(get_rate)
    worker_rate = dev_df['Worker Type'].apply(get_rate)

    D_cal= (driver_rate * dev_df['Nodes'] + 1) * dev_df['hr_per_month'] * dev_df['no_of_Month']
    w_cal = (worker_rate * dev_df['Nodes'] + 1) * dev_df['hr_per_month'] * dev_df['no_of_Month']
    dev_df['DBX'] = D_cal + w_cal
    
    # Update the session state with the calculated DBX values
    st.session_state.dev_costs = dev_df
    return 
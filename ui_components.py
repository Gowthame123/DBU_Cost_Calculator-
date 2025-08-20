# ui_components.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
#from data import  S3_STORAGE_CLASSES
import state as s
from file_exportor import generate_consolidated_excel_export
from calculations import calculate_databricks_costs_for_tier

def render_summary_column(total_cost, databricks_cost, s3_cost, sql_cost, projected_s3_cost_12_months):
    """Renders the right-hand summary column with the donut chart."""
    st.header("📈 Monthly Total")
    st.metric("Total Cloud Cost", f"${total_cost:,.2f}")
    st.divider()

    # Calculate 12-month projected cost (still uses st.session_state.monthly_growth_percent for Databricks)
    projected_cost_12_months = 0
    current_dbx_cost = databricks_cost
    sql_warehouse_cost_fixed = sql_cost # SQL warehouse cost is assumed not to grow with this percentage

    # Re-calculate Databricks 12-month projection using the value from session state
    # (which can be set in state.py or manipulated elsewhere if needed)
    if st.session_state.monthly_growth_percent > 0:
        growth_factor_dbx = 1 + (st.session_state.monthly_growth_percent / 100)
        if growth_factor_dbx != 1:
            projected_dbx_cost_12_months = current_dbx_cost * (growth_factor_dbx**12 - 1) / (growth_factor_dbx - 1)
        else:
            projected_dbx_cost_12_months = current_dbx_cost * 12
    else:
        projected_dbx_cost_12_months = current_dbx_cost * 12

    # Total 12-month projection includes Databricks, SQL, and S3 projections
    #projected_cost_12_months = projected_dbx_cost_12_months + sql_warehouse_cost_fixed * 12 + projected_s3_cost_12_months

    #st.metric("12-Month Projected Total", f"${projected_cost_12_months:,.2f}")
    #st.divider()

    st.header("Cost Distribution")
    cost_data = {
        "Databricks & Compute": databricks_cost,
        "S3 Storage": s3_cost,
        "SQL Warehouse": sql_cost,
    }
    non_zero_costs = {k: v for k, v in cost_data.items() if v > 0}

    if non_zero_costs:
        fig = go.Figure(data=[go.Pie(
            labels=list(non_zero_costs.keys()), values=list(non_zero_costs.values()), hole=.6,
            marker_colors=['#FF8C00', '#3CB371', '#1E90FF'], hoverinfo="label+percent",
            textinfo="percent", textfont_size=14
        )])
        fig.update_layout(
        showlegend=True,
        legend=dict(
            orientation="h",  # Horizontal legend
            yanchor="bottom",
            y=-0.2,  # Adjust this value to move the legend further down
            xanchor="center",
            x=0.5
        ),
        margin=dict(t=0, b=0, l=0, r=0),
        height=250
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("No costs configured yet.")

    st.divider()
    st.header("Cost Insights")
    st.info("""
    - Consider **spot instances** for non-critical workloads to save ~70% on EC2.
    - Enable **auto-suspend** for SQL warehouses to avoid paying for idle compute.
    - Use appropriate **S3 storage classes** for data to optimize storage costs.
    """)

# --- UI Rendering Component ---
#def render_databricks_tab(FLAT_RATE_CARD, FLAT_INSTANCE_LIST, INSTANCE_PRICES, COMPUTE_TYPE_LIST):
def render_databricks_tab():
    """Renders the main Streamlit UI using st.data_editor for inputs, now with tabs."""
    #print(type(FLAT_INSTANCE_LIST))
    st.header("Databricks & Compute Costs")
    st.write('Configure jobs across different tiers. Specify the number of jobs and configure them in the table below.')
    st.write('---')

    # Calculate the grand total for all active tiers
    grand_total_dbx_cost = 0
    grand_total_dbu = 0
    grand_total_ec2_cost = 0
    total_jobs = 0

    # MODIFIED: Moved active_tiers calculation before the metric to use its value
    active_tiers = s.TIERS.copy()
    if 'enable_RAW' in st.session_state and not st.session_state.enable_RAW:
        active_tiers.remove("L0 / Raw")

    for tier in active_tiers:
        # Check and convert to DataFrame if necessary to prevent the error
        jobs_data = st.session_state.dbx_jobs.get(tier, pd.DataFrame())
        if not isinstance(jobs_data, pd.DataFrame):
            jobs_data = pd.DataFrame(jobs_data)
            st.session_state.dbx_jobs[tier] = jobs_data
            
        jobs_df = jobs_data             
        
        _, tier_dbx_cost, tier_ec2_cost, tier_dbu_used = calculate_databricks_costs_for_tier(jobs_df)
        grand_total_dbx_cost += tier_dbx_cost
        grand_total_ec2_cost += tier_ec2_cost
        grand_total_dbu += tier_dbu_used
        total_jobs += len(jobs_df)

    # ADDED: New capsule at the top for summary metrics
    with st.container(border=True):
        col1, col2, col3, col4= st.columns(4)
        col1.metric("Total Jobs", total_jobs)
        col2.metric("Total DBXs", f"{grand_total_dbx_cost:,.2f}")
        col3.metric("EC2 Costs", f"${grand_total_ec2_cost:,.2f}")
        col4.metric("Monthly Total", f"${grand_total_dbx_cost + grand_total_ec2_cost:,.2f}")

    # MODIFIED: Replaced st.checkbox with st.toggle and moved its position
    st.toggle("Enable L0 / RAW", value=True, key='enable_RAW')
        
    for tier in active_tiers:
        with st.container(border=True):
            st.subheader(f"{tier}")
            jobs_df = st.session_state.dbx_jobs.get(tier, pd.DataFrame())

            # This is the original dataframe used to check for changes
            original_jobs_df = jobs_df.copy()

            # Dynamically select the correct compute and instance lists ---
            global_data = st.session_state.global_data
            if tier in ["L0 / Raw", "L1 / Curated"]:
                compute_options = global_data['COMPUTE_TYPES_L0_L1']
                all_instances_for_tier = list(global_data['FLAT_INSTANCE_LIST'].keys())
                instance_prices_for_tier = global_data['INSTANCE_PRICES_L0_L1']
            elif tier == "L2 / Data Product":
                compute_options = global_data['COMPUTE_TYPES_L2']
                instance_prices_for_tier = global_data['INSTANCE_PRICES_L2']
                all_instances_for_tier = list(global_data['FLAT_INSTANCE_LIST'].keys())
            else:
                compute_options = []
                instance_prices_for_tier = {}
                all_instances_for_tier = []

            # --- MODIFIED: Logic to fill in all columns with default values for new rows ---
            for j, row in jobs_df.iterrows():
                # Assign default values for empty numeric columns
                if pd.isna(row['Runtime (hrs)']):
                    jobs_df.at[j, 'Runtime (hrs)'] = 0.0
                if pd.isna(row['Runs/Month']):
                    jobs_df.at[j, 'Runs/Month'] = 0.0
                if pd.isna(row['Nodes']):
                    jobs_df.at[j, 'Nodes'] = 1
                if pd.isna(row['Photon']):
                    jobs_df.at[j, 'Photon'] = tier in ["L0 / Raw", "L1 / Curated"]
                if pd.isna(row['Spot']):
                    jobs_df.at[j, 'Spot'] = tier in ["L0 / Raw", "L1 / Curated"]
                # Assign default value for 'Job Name' if it is empty
                if pd.isna(row['Job Name']) or row['Job Name'] == "":
                    jobs_df.at[j, 'Job Name'] = f"{tier.replace('/', ' ')} Job {j + 1}"
                if pd.isna(row['Compute type']) and compute_options:
                    jobs_df.at[j, 'Compute type'] = compute_options[0]
                selected_compute_type = jobs_df.at[j, 'Compute type']
                selected_instance_type = jobs_df.at[j, 'Instance Type']
                available_instances = list(instance_prices_for_tier.get(selected_compute_type, {}).keys())

                if tier in ["L0 / Raw", "L1 / Curated"]:
                    jobs_df['Photon'] = False
                if tier in ["L0 / Raw", "L1 / Curated"]:
                    jobs_df['Spot'] = False 

                
                if pd.isna(selected_instance_type) or selected_instance_type not in available_instances:
                    new_instance_type = available_instances[0] if available_instances else None
                    jobs_df.at[j, 'Instance Type'] = new_instance_type             
            
            # Get the full DataFrame with calculated costs
            calculated_df, _, _,_ = calculate_databricks_costs_for_tier(jobs_df)
            
            # ADDED: Auto-incrementing Job_Number column on the display DataFrame only.
            calculated_df.insert(1, 'Job_Number', range(1, len(calculated_df) + 1))

            # --- st.data_editor for Job Input and Output ---
            column_config = {
                "Job Name": st.column_config.TextColumn("Job Name"),
                "Job_Number": st.column_config.NumberColumn("Job Number", disabled=True),
                "Runtime (hrs)": st.column_config.NumberColumn("Runtime (hrs)"),
                "Runs/Month": st.column_config.NumberColumn("Runs/Month"),
                 # FIX: Set options to the tier-specific list and make it editable
                "Compute type": st.column_config.SelectboxColumn("Compute type", options=compute_options, disabled=False),
                
                # FIX: Use the dynamic helper function to get instance options
                "Instance Type": st.column_config.SelectboxColumn("Instance Type", options=all_instances_for_tier, required=True),

                #"Compute type": st.column_config.SelectboxColumn("Compute type", options= s.COMPUTE_TYPE_LIST, disabled=False),
                ##"Instance Type": st.column_config.SelectboxColumn("Instance Type", options=list(s.FLAT_INSTANCE_LIST.keys())),
                "Nodes": st.column_config.NumberColumn("Worker_Nodes"),
                "Photon": st.column_config.CheckboxColumn("Photon", disabled =tier in ["L0 / Raw", "L1 / Curated"]),
                "Spot": st.column_config.CheckboxColumn("Spot", disabled =tier in ["L0 / Raw", "L1 / Curated"]),
                "DBU": st.column_config.NumberColumn("DBU", disabled=True, format="%.2f"),
                #"EC2": st.column_config.NumberColumn("EC2", disabled=True, format="$%.2f"),
                "DBX": st.column_config.NumberColumn("DBX", disabled=True, format="$%.2f"),
                "EC2": st.column_config.NumberColumn("EC2", disabled=True, format="$%.2f"),
            }

            edited_df = st.data_editor(
                calculated_df,
                column_config=column_config,
                hide_index=True,
                key=f"data_editor_{tier}",
                use_container_width=True,
                num_rows="dynamic" ,   
                column_order=[
                    "Job Name", "Job_Number", "Runtime (hrs)", "Runs/Month", "Compute type", 
                    "Instance Type", "Nodes", "Photon", "Spot","DBU", "DBX", "EC2"])

            # Update session state with the edited DataFrame's editable columns
            # editable_cols = ["Job Name", "Runtime (hrs)", "Runs/Month", "Compute type", "Instance Type", "Nodes", "Photon", "Spot"]
            # st.session_state.dbx_jobs[tier] = edited_df[editable_cols]
            editable_cols = ["Job Name", "Runtime (hrs)", "Runs/Month", "Compute type", "Instance Type", "Nodes", "Photon", "Spot"]
            if not edited_df[editable_cols].equals(original_jobs_df[editable_cols]):
                 st.session_state.dbx_jobs[tier] = edited_df[editable_cols]
                 st.rerun()
           
            
def render_s3_tab(s3_costs_per_zone, total_s3_cost, projected_s3_cost_12_months):
    """Renders the S3 Storage tab UI with a vertical layout and summary."""
    st.header("AWS S3 Storage Costs")
    st.radio("Calculation Method", ["Direct Storage", "Table-Based"], key="s3_calc_method", horizontal=True)
    
    st.divider()
    # The S3_STORAGE_CLASSES list is assumed to be in the state.py file or a data.py file
    S3_STORAGE_CLASSES = list(st.session_state.global_data.get('S3_PRICING', {}).keys())
    if st.session_state.s3_calc_method == "Direct Storage":
        for zone, config in st.session_state.s3_direct.items():
            with st.container(border=True):
                st.subheader(zone)

                # Get costs from session state for display
                monthly_cost = s3_costs_per_zone.get(zone, 0)
                quarterly_cost = config.get('quarterly_cost', 0)
                half_yearly_cost = config.get('half_yearly_cost', 0)
                
                # Create a row of metrics for cost projections
                metric_col1, metric_col2, metric_col3 = st.columns(3)
                metric_col1.metric("Monthly Cost", f"${monthly_cost:,.2f}")
                metric_col2.metric("Quarterly Cost", f"${quarterly_cost:,.2f}")
                metric_col3.metric("Half-Yearly Cost", f"${half_yearly_cost:,.2f}")

                st.divider()
                
                # Create a row of columns for user inputs
                input_col1, input_col2, input_col3, input_col4 = st.columns(4)
                
                new_class = input_col1.selectbox(
                    "Storage Class", 
                    options=S3_STORAGE_CLASSES, 
                    key=f"s3_class_{zone}", 
                    index=S3_STORAGE_CLASSES.index(config["class"]) if config["class"] in S3_STORAGE_CLASSES else 0
                )
                new_amount = input_col2.number_input("Storage Amount", min_value=0, key=f"s3_amount_{zone}", value=config["amount"])
                new_unit = input_col3.selectbox("Unit", ["GB", "TB"], key=f"s3_unit_{zone}", index=["GB", "TB"].index(config["unit"]))
                new_growth_percent = input_col4.number_input(
                    "Monthly Growth %", 
                    min_value=0.0, max_value=100.0, 
                    value=config.get("monthly_growth_percent", 0.0), 
                    step=0.1, format="%.1f", 
                    key=f"s3_growth_{zone}"
                )

                if (new_class != config["class"] or
                    new_amount != config["amount"] or
                    new_unit != config["unit"] or
                    new_growth_percent != config["monthly_growth_percent"]):

                    st.session_state.s3_direct[zone]["class"] = new_class
                    st.session_state.s3_direct[zone]["amount"] = new_amount
                    st.session_state.s3_direct[zone]["unit"] = new_unit
                    st.session_state.s3_direct[zone]["monthly_growth_percent"] = new_growth_percent
                    st.rerun()
    # else: # Table-Based
    #     st.markdown("Configure S3 storage based on the number of records and columns per table.")

    #     for zone_name, zone_config in st.session_state.s3_table_based.items():
    #         with st.container(border=True):
    #             st.subheader(zone_name)
                
    #             if not isinstance(st.session_state.s3_table_based[zone_name], list):
    #                 old_config = st.session_state.s3_table_based[zone_name]
    #                 st.session_state.s3_table_based[zone_name] = [{
    #                     "Table Name": f"{zone_name.replace(' / ', '_')} Table 1",
    #                     "Records": old_config.get("records", 0),
    #                     "Columns": old_config.get("columns", 0),
    #                     "Table": old_config.get("Table", 1) # Ensure the 'Table' key is added
    #                 }]
    #             current_zone_tables_data = st.session_state.s3_table_based[zone_name]

    #             normalized_current_data = []
    #             for row in current_zone_tables_data:
    #                 normalized_row = row.copy()
    #                 normalized_row["Records"] = float(normalized_row.get("Records") or 0)
    #                 normalized_row["Columns"] = float(normalized_row.get("Columns") or 0)
    #                 normalized_row["Table"] = float(normalized_row.get("Table") or 0)
    #                 normalized_row["Table Name"] = normalized_row.get("Table Name") or ""
    #                 normalized_current_data.append(normalized_row)
                
    #             df_zone_initial = pd.DataFrame(normalized_current_data)
                
    #             if df_zone_initial.empty:
    #                 df_zone_initial = pd.DataFrame(columns=["Table Name", "Records", "Columns", "Table"])
                
    #             display_df = pd.DataFrame(st.session_state.s3_table_based[zone_name])

    #             # --- COMPLETED CODE: Update column_config for the new "Table" column ---
    #             edited_df_zone = st.data_editor(
    #                 display_df,
    #                 column_config={
    #                     "Table Name": st.column_config.TextColumn("Table Name", required=True),
    #                     "Records": st.column_config.NumberColumn("Records", min_value=0, format="%d"),
    #                     "Columns": st.column_config.NumberColumn("Columns", min_value=0, format="%d"),
    #                     "Table": st.column_config.NumberColumn("Number of Tables", min_value=0, format="%d"),
    #                 },
    #                 hide_index=True,
    #                 num_rows="dynamic",
    #                 key=f"s3_table_editor_{zone_name}",
    #                 use_container_width=True
    #             )
                
    #             # Convert Records, Columns, and Table to numeric types, replacing NaNs with 0
    #             for col in ["Records", "Columns", "Table"]:
    #                 if col in edited_df_zone.columns:
    #                     edited_df_zone[col] = pd.to_numeric(edited_df_zone[col], errors='coerce').fillna(0).astype(int)
                
    #             # Sanitize the "Table Name" column
    #             if "Table Name" in edited_df_zone.columns:
    #                 edited_df_zone["Table Name"] = edited_df_zone["Table Name"].fillna('')
                
    #             # Check for changes and update session state
    #             if not edited_df_zone.equals(display_df):
    #                 st.session_state.s3_table_based[zone_name] = edited_df_zone.to_dict(orient='records')
    #                 st.rerun()
                
    #             # --- COMPLETED CODE: Update comparison logic to include the new column ---
    #             if "Records" in edited_df_zone.columns:
    #                 edited_df_zone["Records"] = edited_df_zone["Records"].apply(lambda x: float(x) if x is not None and x != '' else 0.0)
    #             else:
    #                 edited_df_zone["Records"] = 0.0

    #             if "Columns" in edited_df_zone.columns:
    #                 edited_df_zone["Columns"] = edited_df_zone["Columns"].apply(lambda x: float(x) if x is not None and x != '' else 0.0)
    #             else:
    #                 edited_df_zone["Columns"] = 0.0
                
    #             if "Table" in edited_df_zone.columns:
    #                 edited_df_zone["Table"] = edited_df_zone["Table"].apply(lambda x: float(x) if x is not None and x != '' else 0.0)
    #             else:
    #                 edited_df_zone["Table"] = 0.0
                    
    #             if "Table Name" in edited_df_zone.columns:
    #                 edited_df_zone["Table Name"] = edited_df_zone["Table Name"].apply(lambda x: x or "")
    #             else:
    #                 edited_df_zone["Table Name"] = ""

    #             # Filter out empty rows
    #             edited_df_zone_processed = edited_df_zone[
    #                 (edited_df_zone["Table Name"] != "") |
    #                 (edited_df_zone["Records"] != 0) |
    #                 (edited_df_zone["Columns"] != 0) |
    #                 (edited_df_zone["Table"] != 0)
    #             ].reset_index(drop=True)
                

    # st.divider()

    # with st.container(border=True):
    #         st.subheader("Total S3 Storage Cost")
    #         st.markdown(f"<h2 style='text-align: center;'>${total_s3_cost:,.2f}/month</h2>", unsafe_allow_html=True)
    # # with st.container(border=True):
    # #     col1, col2, col3= st.columns(3)
    # #     with col1:
    # #         st.subheader("Total S3 Storage Cost")
    # #         st.markdown(f"<h2 style='text-align: center;'>${total_s3_cost:,.2f}/month</h2>", unsafe_allow_html=True)
    # #     with col2:
    # #         st.subheader("Total Direct Storage Cost")
    # #         st.markdown(f"<h2 style='text-align: center;'>${total_s3_cost:,.2f}/month</h2>", unsafe_allow_html=True)
    # #     with col3:
    # #         st.subheader("Total Table Cost")
    # #         st.markdown(f"<h2 style='text-align: center;'>${total_s3_cost:,.2f}/month</h2>", unsafe_allow_html=True)                        

    else: # Table-Based
        st.markdown("Configure S3 storage based on the number of records and columns per table.")
        for zone_name, zone_config in st.session_state.s3_table_based.items():
            with st.container(border=True):
                st.subheader(zone_name)
                
                # Use a single, clean approach to get the DataFrame
                display_df = pd.DataFrame(st.session_state.s3_table_based[zone_name])
                
                # Render the data editor
                edited_df_zone = st.data_editor(
                    display_df,
                    column_config={
                        "Table Name": st.column_config.TextColumn("Table Name", required=True),
                        "Records": st.column_config.NumberColumn("Records", min_value=0, format="%d"),
                        "Columns": st.column_config.NumberColumn("Columns", min_value=0, format="%d"),
                        "Table": st.column_config.NumberColumn("Number of Tables", min_value=0, format="%d"),
                    },
                    hide_index=True,
                    num_rows="dynamic",
                    key=f"s3_table_editor_{zone_name}",
                    use_container_width=True
                )
                
                # ✅ KEY CHANGE: This is the ONLY place you should check and trigger a rerun.
                # Only compare the edited DataFrame with the original session state data.
                if not edited_df_zone.equals(display_df):
                    # Sanitize the edited DataFrame before storing it
                    edited_df_zone["Records"] = pd.to_numeric(edited_df_zone["Records"], errors='coerce').fillna(0).astype(int)
                    edited_df_zone["Columns"] = pd.to_numeric(edited_df_zone["Columns"], errors='coerce').fillna(0).astype(int)
                    edited_df_zone["Table"] = pd.to_numeric(edited_df_zone["Table"], errors='coerce').fillna(0).astype(int)
                    edited_df_zone["Table Name"] = edited_df_zone["Table Name"].fillna('')
                    
                    # Filter out empty rows
                    sanitized_df = edited_df_zone[
                        (edited_df_zone["Table Name"] != "") |
                        (edited_df_zone["Records"] != 0) |
                        (edited_df_zone["Columns"] != 0) |
                        (edited_df_zone["Table"] != 0)
                    ].reset_index(drop=True)

                    st.session_state.s3_table_based[zone_name] = sanitized_df.to_dict(orient='records')
                    st.rerun()
    st.divider()

    with st.container(border=True):
            st.subheader("Total S3 Storage Cost")
            st.markdown(f"<h2 style='text-align: center;'>${total_s3_cost:,.2f}/month</h2>", unsafe_allow_html=True)                

def render_sql_warehouse_tab(total_sql_cost, total_DBUs):
    """Renders the SQL Warehouse tab UI with a total cost summary."""
    # Retrieve data consistently from session state
    global_data = st.session_state.get('global_data', {})
    sql_warehouse_types = global_data.get('SQL_WAREHOUSE_TYPES_FROM_DATA', [])
    sql_warehouse_sizes_by_type = global_data.get('SQL_WAREHOUSE_SIZES_BY_TYPE', {})

    c1, c2 = st.columns([4, 1])
    with c1:
        st.header("Databricks SQL Warehouse Costs")
    with c2:
        if st.button("＋ Add SQL Warehouse", key="add_sql_warehouse_button_top"):
            new_id = f"warehouse_{len(st.session_state.sql_warehouses)}"
            st.session_state.sql_warehouses.append({
                "id": new_id,
                "name": "New Warehouse",
                "type": sql_warehouse_types[0] if sql_warehouse_types else None,
                "size": next(iter(sql_warehouse_sizes_by_type.get(sql_warehouse_types[0], {})), None),
                'nodes': 1,
                "hours_per_day": 8,
                "days_per_month": 22
            })
            st.rerun()

    st.markdown("---")

    if not st.session_state.sql_warehouses:
        st.info("No SQL Warehouses configured. Click 'Add SQL Warehouse' to start.")
        st.divider()
        return

    for i, warehouse in enumerate(st.session_state.sql_warehouses):
        with st.container(border=True):
            sql_details_col, actions_col = st.columns([4, 1])

            with sql_details_col:
                st.subheader(warehouse["name"])
                
                selected_size_str = warehouse.get("size")
                if selected_size_str is None:
                    st.warning("No size selected for this warehouse.")
                    dbt_per_hr = 0
                    rate_per_hr = 0
                else:

                    # Parse DBUs and Rate from the size string
                    try:
                        parts = selected_size_str.split(" - ")
                        dbt_per_hr = float(parts[1].split(" ")[0]) if len(parts) > 1 else 0
                        rate_per_hr = float(parts[2].split("$")[1].split("/")[0]) if len(parts) > 2 else 0
                    except (IndexError, ValueError):
                        dbt_per_hr = 0
                        rate_per_hr = 0

                st.caption(f"{dbt_per_hr} DBUs • ${rate_per_hr}/hr • {warehouse['hours_per_day']}h/day • {warehouse['days_per_month']} days/month")
            
            with actions_col:
                if st.button("🗑️ Delete", key=f"delete_sql_warehouse_{i}"):
                    st.session_state.sql_warehouses.pop(i)
                    st.rerun()
            
            st.markdown("---")

            c1, c2, c3, c4, c5, c6 = st.columns(6)

            with c1:
                new_name = st.text_input("Name", value=warehouse.get("name", "New Warehouse"), key=f"sql_name_{i}")
            
            with c2:
                current_type = warehouse.get("type")
                type_index = sql_warehouse_types.index(current_type) if current_type in sql_warehouse_types else 0
                new_type = st.selectbox("Compute Type", sql_warehouse_types, index=type_index, key=f"sql_type_{i}")

            with c3:
                available_sizes = list(sql_warehouse_sizes_by_type.get(new_type, {}).keys())
                current_size = warehouse.get("size")
                
                size_index = available_sizes.index(current_size) if current_size in available_sizes else 0
                
                new_size = st.selectbox("Instance", available_sizes, index=size_index, key=f"sql_size_{i}")
            
            with c4:
                new_nodes = st.number_input("Nodes", min_value=0, max_value=24, value=warehouse.get('SQL_nodes', 1), key=f"sql_nodes_{i}")
            with c5:
                new_hours_per_day = st.number_input("Hours/Day", min_value=0.0, max_value=24.0, value=float(warehouse.get('hours_per_day', 0.0)), step=0.5, format="%.1f", key=f"sql_hours_{i}")
            with c6:    
                new_days_per_month = st.number_input("Days/Month", min_value=0, max_value=31, value=warehouse.get("days_per_month", 0), key=f"sql_days_{i}")
            
            if (new_name != warehouse.get("name") or 
                new_type != warehouse.get("type") or
                new_size != warehouse.get("size") or
                new_nodes != warehouse.get("SQL_nodes") or
                new_hours_per_day != warehouse.get("hours_per_day") or
                new_days_per_month != warehouse.get("days_per_month")):
                
                # Update session state with the new values
                warehouse["name"] = new_name
                warehouse["type"] = new_type
                warehouse["size"] = new_size
                warehouse["SQL_nodes"] = new_nodes
                warehouse["hours_per_day"] = new_hours_per_day
                warehouse["days_per_month"] = new_days_per_month
                
                st.rerun()

    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("<h3 style='text-align: center;'>Total SQL Warehouse Cost</h3>", unsafe_allow_html=True)
            warehouse_count = len(st.session_state.sql_warehouses)
            st.markdown(f"<h2 style='text-align: center;'>${total_sql_cost:,.2f}/month</h2>", unsafe_allow_html=True)
            st.caption(f"{warehouse_count} warehouse(s) configured")
        with c2:
            st.markdown("<h3 style='text-align: center;'>Total DBUs</h3>", unsafe_allow_html=True)
            #warehouse_count = len(st.session_state.sql_warehouses)
            st.markdown(f"<h2 style='text-align: center;'>{total_DBUs:,.2f}</h2>", unsafe_allow_html=True)
            #st.caption(f"{warehouse_count} warehouse(s) configured")   

def render_devepoment_tools():
        """Renders the UI for the Development Cost tab."""
        st.header("Development & All-Purpose Compute")
        
        # Retrieve data and define columns
        global_data = st.session_state.global_data
        dev_instance_list = list(global_data.get('FLAT_INSTANCE_LIST_DEV', {}).keys())

        column_config = {
            "Compute_type": st.column_config.TextColumn("Compute Type", disabled=True),
            "Driver type": st.column_config.SelectboxColumn("Driver type", options=dev_instance_list, required=True),
            "Worker Type": st.column_config.SelectboxColumn("Worker Type", options=dev_instance_list, required=True),
            "Nodes": st.column_config.NumberColumn("Worker_Nodes"),
            "hr_per_month": st.column_config.NumberColumn("Hours per Month (hrs)"),
            "no_of_Month": st.column_config.NumberColumn("Number of Month"),
            "DBX": st.column_config.NumberColumn("DBX", disabled=True, format="$%.2f"),
        }
        
        # Create a local copy to perform calculations
        dev_df = st.session_state.dev_costs.copy()
        
        # The calculation is now in calculations.py
        
        # Display the data editor
        edited_df = st.data_editor(
            dev_df,
            column_config=column_config,
            hide_index=True,
            num_rows="dynamic",
            use_container_width=True,
            key="dev_cost_editor"
        )

        # Update session state if the edited DataFrame is different
        if not edited_df.equals(st.session_state.dev_costs):
            st.session_state.dev_costs = edited_df
            st.rerun()
           
def render_configuration_guide():
    """Renders the configuration guide expander at the bottom of a tab."""
    with st.expander("ℹ️ Configuration Guide", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("""
            **Photon Engine** Adds 20% to DBU cost but provides significant performance improvements for analytical workloads.
            """)
            st.markdown("""
            **DBU Rates (Auto-calculated)** Bronze: $0.15, Silver: $0.30, Gold: $0.60 per DBU hour (before Photon premium).
            """)
        with c2:
            st.markdown("""
            **Spot Instances** Provides ~70% cost savings on EC2 compute but instances may be interrupted.
            """)
            st.markdown("""
            **Instance Families** Choose instance types based on workload: General Purpose (`m5`), Compute Optimized (`c5`), Memory Optimized (`r5`/`r5d`).
            """)

def render_export_button(calculated_dbx_data, s3_calc_method, s3_direct_config, s3_table_based_config, sql_warehouses_config):
    """
    Renders the Excel export button. This function is called from main.py.
    It orchestrates the data collection from session state and passes it
    to the excel_exporter for file generation.
    """
    # Generate Excel file content
    excel_file_bytes = generate_consolidated_excel_export(
        calculated_dbx_data,
        s3_calc_method,
        s3_direct_config,
        s3_table_based_config,
        sql_warehouses_config
    )

    # Export Button (visible)
    st.download_button(
        label="📊 Export Excel",
        data=excel_file_bytes,
        file_name="cloud_cost_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="export_consolidated_excel_button"
    )

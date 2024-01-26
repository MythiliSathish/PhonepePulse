import os
import json
import pandas as pd
import numpy as np
import pymysql 
import seaborn as sns
import mysql.connector
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
import plotly.express as px
from streamlit_option_menu import option_menu



# aggre_transaction:

path1="C:/Users/dell/Desktop/project/phonepe/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list=os.listdir(path1)

columns1={"states":[], "years":[], "quarter":[], "transaction_type":[], "transaction_count":[], "transaction_amount":[]}

for state in agg_tran_list:                # location for state
    cur_states=path1+state+"/"
    agg_year_list=os.listdir(cur_states)   # loction 
    
    for year in  agg_year_list:            # location for year
         cur_year=cur_states+year+"/"
         agg_file_list=os.listdir(cur_year)     

         for file in agg_file_list:
              cur_file=cur_year+file
              data=open(cur_file,"r")       # read file

              A=json.load(data)

              # get data by using slicing:

              for i in A["data"]["transactionData"]:
                    name=i["name"]
                    count=i["paymentInstruments"][0]["count"]
                    amount=i["paymentInstruments"][0]["amount"]
                    columns1["transaction_type"].append(name)
                    columns1["transaction_count"].append(count)
                    columns1["transaction_amount"].append(amount)
                    columns1["states"].append(state)
                    columns1["years"].append(year)
                    columns1["quarter"].append(int(file.strip(".json")))        


# create agg_tran dataframe:

agg_tran=pd.DataFrame(columns1)                   

# aggre_user:

path2="C:/Users/dell/Desktop/project/phonepe/pulse/data/aggregated/user/country/india/state/"
agg_user_list=os.listdir(path2)

columns2={"states":[], "years":[], "quarter":[], "Brands":[], "transaction_count":[], "percentage":[]}

for state in agg_user_list:                # location for state
    cur_states=path2+state+"/"
    agg_year_list=os.listdir(cur_states)   # loction 
    
    for year in  agg_year_list:            # location for year
         cur_year=cur_states+year+"/"
         agg_file_list=os.listdir(cur_year)     

         for file in agg_file_list:
              cur_file=cur_year+file
              data=open(cur_file,"r")       # read file

              B=json.load(data)


              try:
                    
                    for i in B["data"]["usersByDevice"]:
                              brand=i["brand"]
                              count=i["count"]
                              percentage=i["percentage"]
                              columns2["Brands"].append(brand)
                              columns2["transaction_count"].append(count)
                              columns2["percentage"].append(percentage)
                              columns2["states"].append(state)
                              columns2["years"].append(year)
                              columns2["quarter"].append(int(file.strip(".json")))   

              except:
                    pass    
              

# create agg_user dataframe:

agg_user=pd.DataFrame(columns2)

# map_transaction:

path3="C:/Users/dell/Desktop/project/phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list= os.listdir(path3)


columns3={"states":[], "years":[], "quarter":[], "districts":[], "transaction_count":[], "transaction_amount":[]}

for state in map_tran_list:                # location for state
    cur_states=path3+state+"/"
    agg_year_list=os.listdir(cur_states)   # loction 
    
    for year in  agg_year_list:            # location for year
         cur_year=cur_states+year+"/"
         agg_file_list=os.listdir(cur_year)     

         for file in agg_file_list:
              cur_file=cur_year+file
              data=open(cur_file,"r")       # read file

              C=json.load(data)
              for i in C["data"]["hoverDataList"]:
                   name=i["name"]
                   count=i["metric"][0]["count"]
                   amount=i["metric"][0]["amount"]
                   columns3["districts"].append(name)
                   columns3["transaction_count"].append(count)
                   columns3["transaction_amount"].append(amount)
                   columns3["states"].append(state)
                   columns3["years"].append(year)
                   columns3["quarter"].append(int(file.strip(".json")))

# create map_tran dataframe:
                   
map_tran=pd.DataFrame(columns3)

# map_user:

path4="C:/Users/dell/Desktop/project/phonepe/pulse/data/map/user/hover/country/india/state/"
map_user_list=os.listdir(path4)


columns4={"states":[], "years":[], "quarter":[], "districts":[], "registeredUsers":[], "appOpens":[]}
for state in map_user_list:
    cur_states=path4+state+"/"
    agg_year_list=os.listdir(cur_states)   # loction 
    
    for year in  agg_year_list:            # location for year
         cur_year=cur_states+year+"/"
         agg_file_list=os.listdir(cur_year)     

         for file in agg_file_list:
              cur_file=cur_year+file
              data=open(cur_file,"r")       # read file

              D=json.load(data)
              
              for i in D["data"]["hoverData"].items():
                    district=i[0]
                    registeredUsers=i[1]["registeredUsers"]
                    appOpens=i[1]["appOpens"]
                    columns4["districts"].append(district)
                    columns4["registeredUsers"].append(registeredUsers)
                    columns4["appOpens"].append(appOpens)
                    columns4["states"].append(state)
                    columns4["years"].append(year)
                    columns4["quarter"].append(int(file.strip(".json")))


# create map_user dataframe:
                    
map_user=pd.DataFrame(columns4)

# top_transaction:

path5="C:/Users/dell/Desktop/project/phonepe/pulse/data/top/transaction/country/india/state/"
top_tran_list=os.listdir(path5)

column5={"states":[], "years":[], "quarter":[], "district":[], "transaction_count":[], "transaction_amount":[] }
for state in top_tran_list:
    cur_states=path5+state+"/"
    top_year_list=os.listdir(cur_states)

    for year in top_year_list:
        cur_year=cur_states+year+"/"
        top_file_list=os.listdir(cur_year)

        for file in top_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")   

            E=json.load(data)

            for i in E["data"]["districts"]:
                name=i["entityName"]
                count=i["metric"]["count"]
                amount=i["metric"]["amount"]
                column5["district"].append(name)
                column5["transaction_count"].append(count)
                column5["transaction_amount"].append(amount)
                column5["states"].append(state)
                column5["years"].append(year)
                column5["quarter"].append(int(file.strip(".json")))
                     
# create top_tran dataframe:
                
top_tran=pd.DataFrame(column5)

# top_user:

path6="C:/Users/dell/Desktop/project/phonepe/pulse/data/top/user/country/india/state/" 
top_user_list=os.listdir(path6)

column6={"states":[], "years":[], "quarter":[], "district":[], "registeredUsers":[]}

for state in top_user_list:
    cur_states=path6+state+"/"
    top_year_list=os.listdir(cur_states)

    for year in top_year_list:
        cur_year=cur_states+year+"/"
        top_file_list=os.listdir(cur_year)

        for file in top_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            F=json.load(data)

            for i in F["data"]["districts"]:
                name=i["name"]
                registeredUsers=i["registeredUsers"]
                column6["district"].append(name)
                column6["registeredUsers"].append(registeredUsers)
                column6["states"].append(state)
                column6["years"].append(year)
                column6["quarter"].append(file)

# create top_user dataframe:
                
top_user=pd.DataFrame(column6)

# connection between python & mysql:

connect=pymysql.connect(host="127.0.0.1", user="root", passwd="mysql@123")
cur=connect.cursor()

# create sql database:
try:
    cur.execute("create database phonepe")
except:
     print("database already created")


# create agg_tran table:

connect=pymysql.connect(host="127.0.0.1", user="root", passwd="mysql@123",database="phonepe")
cur=connect.cursor()

try:
    cur.execute("create table agg_tran(states text,years text,quarter int,transaction_type text,transaction_count int,transaction_amount float)")

    # insert values to mysql:
    sql="insert into agg_tran values(%s,%s,%s,%s,%s,%s)"

    for i in range(0,len(agg_tran)):
        cur.execute(sql,tuple(agg_tran.iloc[i]))
        connect.commit()
        
except:
    print("agg_tran table already created")


# create agg_user table:

connect=pymysql.connect(host="127.0.0.1", user="root", passwd="mysql@123",database="phonepe")
cur=connect.cursor()

# create table:
try:
    cur.execute("create table agg_user(states text,years text,quarter int,Brands text,transaction_count int,percentage float)")

    # allocate space for insert values:

    sql="insert into agg_user values(%s,%s,%s,%s,%s,%s)"

    # insert values:

    for i in range(0,len(agg_user)):
        cur.execute(sql,tuple(agg_user.iloc[i]))
        connect.commit()

except:
     print("agg_user table already created")
# create map_tran table:

connect=pymysql.connect(host="127.0.0.1", user="root", passwd="mysql@123", database="phonepe")
cur=connect.cursor()

# create table:

try:
    cur.execute("create table map_tran(states text,years text,quarter int,districts text,transaction_count int,transaction_amount float)")

    # allocate space:

    sql="insert into map_tran values(%s,%s,%s,%s,%s,%s)"

    # insert values:

    for i in range(0,len(map_tran)):
        cur.execute(sql,tuple(map_tran.iloc[i]))
        connect.commit()
except:
     print("map_tran table already created")

# create map_user table:

connect=pymysql.connect(host="127.0.0.1", user="root", passwd="mysql@123",database="phonepe")
cur=connect.cursor()

# create table:

try:
    cur.execute("create table map_user(states text,years text,quarter int,districts text,registeredUsers int,appOpens int)")

    # allocate space:

    sql="insert into map_user values(%s,%s,%s,%s,%s,%s)"

    # insert values:

    for i in range(0,len(map_user)):
        cur.execute(sql,tuple(map_user.iloc[i]))
        connect.commit()

except:
     print("map_user table already created")

# create top_tran table:

connect=pymysql.connect(host="127.0.0.1",user="root",passwd="mysql@123",database="phonepe")
cur=connect.cursor()

# create table:

try:
    cur.execute("create table top_tran(states text,years text,quarter int,district text,transaction_count int,transaction_amount float)")

    # alloacte space:

    sql="insert into top_tran values(%s,%s,%s,%s,%s,%s)"

    # insert values:

    for i in range(0,len(top_tran)):
        cur.execute(sql,tuple(top_tran.iloc[i]))
        connect.commit()
except:
     print("top_tran table already created")

# create top_user table:

connect=pymysql.connect(host="127.0.0.1",user="root",passwd="mysql@123",database="phonepe")
cur=connect.cursor()

# create table:

try:
    cur.execute("create table top_user(states text,years text,quarter text,district text,registeredUsers int)")

    # allocate space:

    sql="insert into top_user values(%s,%s,%s,%s,%s)"

    # insert values:

    for i in range(0,len(top_user)):
        cur.execute(sql,tuple(top_user.iloc[i]))
        connect.commit()
except:
     print("top_user table already created")




# STREAMLIT:
    
st.set_page_config("Phonepe_data",page_icon="bar_chart:",layout="wide",initial_sidebar_state= "expanded")

st.title(":violet[Phonepe Pulse Data Visualization]")
          
st.sidebar.title(":blue[Phonepe Pulse Data Visualization:]")

# option_menu:

with st.sidebar:
    selected = option_menu("Menu", ["Home","Top Charts","Explore Data"], 
                icons=["house","graph-up-arrow","bar-chart-line", "exclamation-circle"],
                menu_icon= "menu-button-wide",
                default_index=0,
                styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                        "nav-link-selected": {"background-color": "#6F36AD"}})


# MENU 1-HOME:

if selected=="Home":
    st.markdown("### :green[Data Visualization and Exploration]")
    st.markdown("### :green[A User-Friendly Tool Using Streamlit and Plotly]")
    col1,col2 = st.columns([3,2],gap="medium")
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("### :violet[Domain :] Fintech")
        st.markdown("### :violet[Technologies used :] Github Cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.")
        st.markdown("### :violet[Overview :] In this streamlit web app you can visualize the phonepe pulse data and gain lot of insights on transactions, number of users, top 10 state, district, pincode and which brand has most number of users and so on. Bar charts, Pie charts and Geo map visualization are used to get some insights.")

# MENU 2-TOP CHARTS:
if selected=="Top Charts":
     st.markdown("### :orange[Top Charts]")
     type=st.selectbox("**Type**",("Transactions","Users"))
            
     year=st.sidebar.slider("**year**",min_value=2018,max_value=2022)
     quarter=st.sidebar.slider("**quarter**",min_value=1,max_value=4)

# select transactions charts:
if type == "Transactions":
    col1,col2=st.columns([1,1],gap="small")
    with col1:
        
            st.markdown("## :violet[State]")
            cur.execute(f'select states,sum(transaction_count) as total_transaction_count, sum(transaction_amount) as total_transaction_amount from aggre_tran where years={year} and quarter={quarter} group by states order by total_transaction_amount desc limit 10')
            df=pd.DataFrame(cur.fetchall(),columns=["states","transaction_count","total_amount"])
            fig=px.pie(df, values="total_amount",
                    names="states",
                    title="Top 10",
                    color_discrete_sequence=px.colors.sequential.Agsunset,
                    hover_data=["transaction_count"],
                    labels={"transaction_count":"transaction_count"})

            st.plotly_chart(fig)

    with col2:
        
            st.markdown("## :violet[District]")
            cur.execute(f'select districts,sum(transaction_count) as transaction_count, sum(transaction_amount) as total_amount from map_trans where years={year} and quarter={quarter} group by districts order by total_amount desc limit 10')
            df=pd.DataFrame(cur.fetchall(),columns=["districts","transaction_count","total_amount"])
            fig=px.pie(df, values="total_amount",
                    names="districts",
                    title="Top 10",
                    color_discrete_sequence=px.colors.sequential.Agsunset,
                    hover_data=['transaction_count'],
                    labels={'transaction_count':'transaction_count'})

            st.plotly_chart(fig)

# select user charts:   
if type=="Users":
     col1,col2,col3=st.columns([2,2,2],gap="small")
     with col1:
         
        st.markdown("## :violet[Brands]")
        if year == 2022 and quarter in [2,3,4]:
            st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4")
        else:
            cur.execute(f"select Brands, sum(transaction_count) as Total_Count, avg(percentage)*100 as Avg_Percentage from aggre_user where years = {year} and quarter = {quarter} group by Brands order by Total_Count desc limit 10")
            df = pd.DataFrame(cur.fetchall(), columns=['Brand', 'Total_Users','Avg_Percentage'])
            fig = px.bar(df,
                        title='Top 10',
                        x="Total_Users",
                        y="Brand",
                        orientation='h',
                        color='Avg_Percentage',
                        color_continuous_scale=px.colors.sequential.Agsunset)
            st.plotly_chart(fig,use_container_width=True)  

     with col2:
          st.markdown("## :violet[District]")
          cur.execute(f'select districts, sum(registeredUsers) as Total_users, sum(appOpens) as totalAppOpens from map_users where years={year} and quarter={quarter} group by districts order by Total_users desc limit 10')
          df = pd.DataFrame(cur.fetchall(), columns=['District', 'Total_Users','totalAppOpens'])
          df.Total_Users = df.Total_Users.astype(float)
          fig = px.bar(df,
                      title='Top 10',
                      x="Total_Users",
                      y="District",
                      orientation='h',
                      color='Total_Users',
                      color_continuous_scale=px.colors.sequential.Agsunset)
          st.plotly_chart(fig,use_container_width=True)
        
     with col3:
          st.markdown("## :violet[State]")
          cur.execute(f'select states, sum(registeredUsers) as Total_users, sum(appOpens) as totalAppOpens from map_users where years={year} and quarter={quarter} group by states order by Total_users desc limit 10')
          df = pd.DataFrame(cur.fetchall(), columns=['states', 'Total_Users','TotalAppOpens'])
          df.Total_Users = df.Total_Users.astype(float)
          fig = px.bar(df,
                      title='Top 10',
                      x="Total_Users",
                      y="states",
                      orientation='h',
                      color='Total_Users',
                      color_continuous_scale=px.colors.sequential.Agsunset)
          st.plotly_chart(fig,use_container_width=True)
        
                    
       
if selected=="Explore Data":

    year=st.sidebar.selectbox(":violet[Select an option from year:]",("2018","2019","2020","2021","2022"))
    quarter=st.sidebar.selectbox(":violet[Select an option from quarter:]",('1','2','3','4'))
    type=st.sidebar.selectbox("**Type**",("Transactions","Users"))
    col1,col2=st.columns(2)
    if type == "Transactions":
        with col1:
                st.markdown("## :violet[Overall State Data-Transactions Amount]")
                cur.execute(f'select states, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_trans where years={year} and quarter={quarter} group by states order by states')
                df1=pd.DataFrame(cur.fetchall(),columns=["state","Total_transaction","Total_amount"])

                
                fig = px.choropleth(
                df1,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state',
                color="Total_amount",
                color_continuous_scale='Reds',
                title="Overall Transaction Amount"
                )

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig)


        with col2:
                st.markdown("## :violet[Overall State Data-Transactions Count]")
                cur.execute(f'select states, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_trans where years={year} and quarter={quarter} group by states order by states')
                df1=pd.DataFrame(cur.fetchall(),columns=["state","Total_transaction","Total_amount"])

                
                fig = px.choropleth(
                df1,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state',
                color="Total_transaction",
                color_continuous_scale='Reds',
                title="Overall Transaction Count"
                )

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig)
                            

    if type == "Users":
        col1,col2=st.columns(2)
        with col1:
               st.markdown("## :violet[Overall State Data-AppOpens]")
               cur.execute(f"select states, sum(registeredUsers) as Total_users, sum(appOpens) as TotalAppOpens from map_users where years={year} and quarter={quarter} group by states,districts,years order by states")
               df1=pd.DataFrame(cur.fetchall(),columns=["states","TotalAppOpens","Total_users"])
               fig = px.choropleth(
               df1,
               geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
               featureidkey='properties.ST_NM',
               locations='states',
               color="TotalAppOpens",
               color_continuous_scale='Reds',
               title="Overall AppOpens"
               )
        
               fig.update_geos(fitbounds="locations", visible=False)
               st.plotly_chart(fig)

        with col2:
               st.markdown("## :violet[Overall State Data-AppOpens]")
               cur.execute(f"select states, sum(registeredUsers) as Total_users, sum(appOpens) as TotalAppOpens from map_users where years={year} and quarter={quarter} group by states,years,quarter,districts order by states,districts")
               df1=pd.DataFrame(cur.fetchall(),columns=["states","Total_users","TotalAppOpens"])
               fig = px.choropleth(
               df1,
               geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
               featureidkey='properties.ST_NM',
               locations='states',
               color="Total_users",
               color_continuous_scale='Reds',
               title="Overall Users "
               )
        
               fig.update_geos(fitbounds="locations", visible=False)
               st.plotly_chart(fig)
                


     


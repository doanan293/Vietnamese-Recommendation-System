import streamlit as st
import pandas as pd
import statsmodels.api as sm
import plotly.graph_objs as go
import os
from datetime import datetime
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import talib as ta
from talib import MA_Type
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score


# Add background and custom styles via CSS
def add_bg_and_custom_styles():
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: #ffffff;
            background-image: radial-gradient(#000000 0.75px, #ffffff 0.75px);
            background-size: 15px 15px;
        }}
        .css-18e3th9 {{
            padding: 10px;
            background-color: #f0f2f6;
            border-radius: 10px;
            margin: 10px;
        }}
        .css-1aumxhk {{
            font-size: 18px;
            color: #333;
        }}
        .css-1aumxhk input {{
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 5px;
            font-size: 16px;
            width: 100%;
            margin-bottom: 10px;
        }}
        .st-emotion-cache-1aumxhk {{
            color: #FFFFFF !important;
            font-weight: bold !important;
        }}
        .st-emotion-cache-1aumxhk input {{
            color: #FFFFFF !important;
        }}
        .element-container {{
            color: #FFFFFF !important;
            font-weight: bold !important;
        }}
        .st.sidebar.header {{
            color: #FFFFFF !important;
            font-weight: bold !important;
        }}
        .main-content h1, .main-content h2, .main-content h3, .main-content h4, .main-content h5, .main-content h6 {{
            color: black !important;
        }}
        .main-content p {{
            color: black !important;
        }}
        .news-title {{
            font-size: 18px;
            font-weight: bold;
            color: black !important;
            text-decoration: none;
            display: block;
            margin-bottom: 1px;
        }}
        .news-date {{
            font-size: 14px;
            color: grey;
            margin-bottom: 1px;
        }}
        hr {{
            border: 0;
            height: 1px;
            background: #333;
            background-image: linear-gradient(to right, #ccc, #333, #ccc);
            margin-bottom: 20px;
        }}
        .tab-content {{
            margin: 20px;
        }}
        .stTabs {{
            margin-bottom: 10px;
        }}
        .stTabs .stTab {{
            padding: 10px;
            margin: 5px;
            background-color: #f0f2f6;
            border-radius: 5px;
        }}
        .stTabs .stTab:hover {{
            background-color: #e0e2e6;
        }}
        .stTabs .stTab.active {{
            background-color: #d0d2d6;
        }}
        /* Custom Sidebar Styles */
        .st-emotion-cache-1gv3huu {{
            background-color: #E6F7FF !important;
            padding: 20px !important;
            border-radius: 10px !important;
            border: 2px solid #BDC3C7 !important;
        }}
        .st-emotion-cache-dvne4q {{
            color: #ECF0F1 !important;
        }}
        .st-emotion-cache-1aumxhk {{
            font-size: 22px !important;
            color: #FFFFFF !important;
            font-weight: bold !important;
            text-align: center !important;
            margin-bottom: 20px !important;
        }}
        .st-emotion-cache-1avcm0n {{
            color: #E6F7FF !important;
            background-color: #FFFFFF !important;
            border: 2px solid #BDC3C7 !important;
            border-radius: 10px !important;
            padding: 10px !important;
            font-size: 16px !important;
        }}
        .st-emotion-cache-1avcm0n:hover {{
            border: 2px solid #888888 !important;
        }}
        .css-1aumxhk {{
            color: #FFFFFF !important;  /* Set text color to white */
        }}
        .st-emotion-cache-183lzff.exotz4b0 {{
            color: black !important;  /* Set text color to black */
            background-color: white !important;
            padding: 10px;
            border-radius: 10px;
            border: 2px solid #ddd;
            margin: 10px 0;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )


# Apply the custom styles
add_bg_and_custom_styles()


def get_price_from_csv(stock_code, folder_path='D:\\Study Program\\Project\\Price'):
    csv_file = os.path.join(folder_path, f'{stock_code}_Price.csv')
    try:
        df = pd.read_csv(csv_file, index_col=False)
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')  # Adjusting the date format
        df = df.dropna(subset=['Date'])  # Remove any rows with invalid dates

        # Remove commas and convert Price-related columns to float
        df['Close'] = df['Price'].str.replace(',', '').astype(float)
        df['Open'] = df['Open'].str.replace(',', '').astype(float)
        df['High'] = df['High'].str.replace(',', '').astype(float)
        df['Low'] = df['Low'].str.replace(',', '').astype(float)

        df = df.sort_values(by='Date', ascending=True).reset_index(drop=True)
        return df
    except FileNotFoundError:
        return None
def get_quarter_report_from_csv(stock_code, folder_path='D:\\Study Program\\Project\\Quarter_report'):
    csv_file = os.path.join(folder_path, f'{stock_code}_quarter_report.csv')
    try:
        df = pd.read_csv(csv_file)
        return df
    except FileNotFoundError:
        return None
# Function to read news data from CSV in a folder
def get_news_from_csv(stock_code, folder_path='D:\\Study Program\\Project\\News'):
    csv_file = os.path.join(folder_path, f'{stock_code}_news.csv')
    try:
        df = pd.read_csv(csv_file)
        return df
    except FileNotFoundError:
        return pd.DataFrame()



# Function to perform OLS regression and plot candlestick chart
def perform_ols_and_plot(stock_code):
    file_path1 = f'D:\\Study Program\\Project\\Price\\{stock_code}_Price.csv'
    file_path2 = f'D:\\Study Program\\Project\\News\\{stock_code}_news.csv'

    df1 = pd.read_csv(file_path1)
    df2 = pd.read_csv(file_path2)

    # Convert Date and Time to the same format
    df1['Date'] = pd.to_datetime(df1['Date'])
    df2['Time'] = pd.to_datetime(df2['Time'], format="%d/%m/%Y %H:%M")

    # Extract the date part from df2['Time']
    df2['Time'] = df2['Time'].dt.date

    # Perform the left join
    merged_df = pd.merge(df1, df2, left_on=df1['Date'].dt.date, right_on='Time', how='left')

    # Drop the redundant column 'key_0' if needed
    merged_df.drop(columns=['Link', 'Content'], inplace=True)

    # Check if 'Evaluation' column exists, if not create it
    if 'Evaluation' not in merged_df.columns:
        merged_df['Evaluation'] = 0

    # Fill NaN values in the 'Evaluation' column with 0
    merged_df['Evaluation'].fillna(0, inplace=True)
    merged_df = merged_df.sort_values(by='Date')

    # Remove commas and convert Price-related columns to float
    merged_df['Close'] = merged_df['Price'].str.replace(',', '').astype(float)
    merged_df['Open'] = merged_df['Open'].str.replace(',', '').astype(float)
    merged_df['High'] = merged_df['High'].str.replace(',', '').astype(float)
    merged_df['Low'] = merged_df['Low'].str.replace(',', '').astype(float)

    # Insert a new column 'Period' at the first position with values ranging from 1 to the length of the DataFrame
    merged_df.insert(0, 'Period', range(1, len(merged_df) + 1))

    # Drop the column 'Change %'
    df = merged_df.drop(columns=['Change %', "Price"])

    df['Month'] = pd.to_datetime(df['Date']).dt.month
    # Create dummy variables for 'Month'
    df = pd.get_dummies(df, columns=['Month'], drop_first=True)

    # Convert boolean columns to numeric (0 and 1) directly
    bool_columns = df.select_dtypes(include='bool').columns
    df[bool_columns] = df[bool_columns].astype(int)

    # Prepare the dependent and independent variables
    X = df[['Period'] + [col for col in df.columns if 'Month_' in col] + ['Evaluation']]
    y = df['Close']

    # Add a constant to the independent variables
    X = sm.add_constant(X)

    # Fit the multiple regression model
    model = sm.OLS(y, X).fit()

    # Plot candlestick chart
    fig_candlestick = go.Figure(data=[go.Candlestick(x=merged_df['Date'],
                                                     open=merged_df['Open'],
                                                     high=merged_df['High'],
                                                     low=merged_df['Low'],
                                                     close=merged_df['Close'])])
    fig_candlestick.update_layout(
        title='Candlestick chart',
        xaxis_title='Date',
        yaxis_title='Price',
        xaxis_rangeslider_visible=False
    )

    # Calculate RSI
    df['RSI'] = ta.RSI(df['Close'], 21)

    # Plot RSI
    fig_rsi = make_subplots(rows=1, cols=1)
    fig_rsi.add_trace(go.Scatter(x=df['Date'], y=df['RSI'], name='RSI'))
    fig_rsi.add_hline(y=30, line_dash='dash', line_color='limegreen', line_width=1)
    fig_rsi.add_hline(y=70, line_dash='dash', line_color='red', line_width=1)
    fig_rsi.update_yaxes(title_text='RSI Score')

    # Adds the range selector
    fig_rsi.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label='1m', step='month', stepmode='backward'),
                    dict(count=6, label='6m', step='month', stepmode='backward'),
                    dict(count=1, label='YTD', step='year', stepmode='todate'),
                    dict(count=1, label='1y', step='year', stepmode='backward'),
                    dict(step='all')
                ]),
            type='date')
    )

    # Set the color from white to black on range selector buttons
    fig_rsi.update_layout(xaxis=dict(rangeselector=dict(font=dict(color='black'))))

    # Sets customized padding
    fig_rsi.update_layout(margin=go.layout.Margin(r=10, b=10))

    layout = go.Layout(template='plotly_dark', title=f"{stock_code} - RSI", height=500, legend_title='Legend')
    fig_rsi.update_layout(layout)

    # Calculate Bollinger Bands
    df['BU'], df['BM'], df['BL'] = ta.BBANDS(df.Close, timeperiod=20, matype=MA_Type.EMA)
    fig_bb = px.line(data_frame=df, x=df.index, y=['Close', 'BU', 'BM', 'BL'])

    # Update y & x axis labels
    fig_bb.update_yaxes(title_text='Price')
    fig_bb.update_xaxes(title_text='Date')

    fig_bb.data[0].name = 'Price'

    # Sets customized padding
    fig_bb.update_layout(margin=go.layout.Margin(r=10, b=10))

    layout = go.Layout(template='plotly_dark', title=f"{stock_code}" + ' - Price, Bollinger Bands', height=500, legend_title='Legend')
    fig_bb.update_layout(layout)

    # Drop Buy and Sell columns if they exist
    df.drop(['Buy', 'Sell'], inplace=True, axis=1, errors='ignore')

    # Create DataFrame
    df_buy = df.query('Low < BL')[['Date', 'Close']]
    df_sell = df.query('High > BU')[['Date', 'Close']]

    # Round close values for both buy and sell
    df_buy['Close'] = round(df_buy.Close.round())
    df_sell['Close'] = round(df_sell.Close.round())

    fig_bs = go.Figure(data=[go.Candlestick(x=df['Date'], open=df['Open'],
                                         high=df['High'],
                                         low=df['Low'],
                                         close=df['Close'],
                                         name='Candlestick')])

    # Plot BU line graph; don't show legend
    fig_bs.add_trace(go.Scatter(x=df['Date'], y=df['BU'],
                             fill=None, mode='lines', showlegend=False))

    # Plot BL line graph and fill upto BU; don't show legend
    fig_bs.add_trace(go.Scatter(x=df['Date'], y=df['BL'],
                             fill='tonexty', mode='lines', showlegend=False))

    # Plot Buy signals
    fig_bs.add_trace(go.Scatter(x=df_buy['Date'], y=df_buy['Close'], mode='markers',
                             marker=dict(symbol='x', size=7, line=dict(width=1)), name='Buy'))

    # Plot Sell Signls
    fig_bs.add_trace(go.Scatter(x=df_sell['Date'], y=df_sell['Close'], mode='markers',
                             marker=dict(symbol='diamond', size=7, line=dict(width=1)), name='Sell'))

    fig_bs.update_yaxes(title_text='Price')
    fig_bs.update_xaxes(title_text='Date')

    fig_bs.data[0].name = 'Price'
    fig_bs.update_layout(margin=go.layout.Margin(r=10, b=10))

    layout = go.Layout(template='plotly_dark',
                       title=f"{stock_code}" + ' - Buy / Sell Signals', height=500,
                       xaxis_rangeslider_visible=False)
    fig_bs.update_layout(layout)

    # Calculate MACD values
    # Empty Data Frame to collect MACD analysis results
    analysis = pd.DataFrame()
    analysis['macd'], analysis['macdSignal'], analysis['macdHist'] = ta.MACD(df.Close,
                                                                             fastperiod=12,
                                                                             slowperiod=26,
                                                                             signalperiod=9)
    fig_ma = make_subplots(rows=2, cols=1)

    # Candlestick chart for pricing
    fig_ma.append_trace(go.Candlestick(x=df['Date'], open=df['Open'],
                                    high=df['High'], low=df['Low'],
                                    close=df['Close'], showlegend=False),
                     row=1, col=1)

    # Fast Signal (%k)
    fig_ma.append_trace(go.Scatter(x=df['Date'],
                                y=analysis['macd'],
                                line=dict(color='#C42836', width=1),
                                name='MACD Line'), row=2, col=1)

    # Slow signal (%d)
    fig_ma.append_trace(go.Scatter(x=df['Date'], y=analysis['macdSignal'],
                                line=dict(color='limegreen', width=1),
                                name='Signal Line'), row=2, col=1)

    # Colorize the histogram values
    colors = np.where(analysis['macd'] < 0, '#EA071C', '#57F219')

    # Plot the histogram
    fig_ma.append_trace(go.Bar(x=df['Date'], y=analysis['macdHist'],
                            name='Histogram', marker_color=colors),
                     row=2, col=1)

    fig_ma['layout']['yaxis']['title'] = 'Price'
    fig_ma['layout']['xaxis2']['title'] = 'Date'

    fig_ma.data[0].name = 'Price'

    # Sets customized padding
    fig_ma.update_layout(margin=go.layout.Margin(r=10, b=10))

    # Make it pretty
    layout = go.Layout(template='plotly_dark', title=f"{stock_code}" + ' - MACD Indicator', height=700,
                       xaxis_rangeslider_visible=False)

    # Update options and show plot
    fig_ma.update_layout(layout)

    fig_ma.update_layout(legend=dict(yanchor="top", y=0.45, xanchor="left", x=1.01))

    return model.summary(), fig_candlestick, fig_rsi, fig_bb, fig_bs, fig_ma


# Display stock price information
def display_stock_price_info(stock_code):
    stock_code = stock_code.upper()
    price_data = get_price_from_csv(stock_code)

    if price_data is not None:
        st.subheader(f"Thông tin giá cho cổ phiếu {stock_code}: ")

        # Date selection
        min_date = price_data['Date'].min()
        max_date = price_data['Date'].max()

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.selectbox('Chọn ngày bắt đầu',
                                      options=pd.date_range(min_date, max_date).strftime("%Y-%m-%d"))
        with col2:
            end_date = st.selectbox('Chọn ngày kết thúc',
                                    options=pd.date_range(min_date, max_date).strftime("%Y-%m-%d"))

        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        filtered_price_data = price_data[(price_data['Date'] >= start_date) & (price_data['Date'] <= end_date)]

        st.write(filtered_price_data)

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(x=filtered_price_data['Date'], y=filtered_price_data['Close'], mode='lines', name='Close'))
        fig.update_layout(
            title=f'Giá đóng cửa của cổ phiếu {stock_code} theo thời gian',
            xaxis_title='Ngày',
            yaxis_title='Giá',
            xaxis=dict(
                rangeslider=dict(visible=True),
                type='date',
                tickformat='%Y-%m-%d',
                showgrid=False,
                color='black'
            ),
            yaxis=dict(
                showgrid=False,
                color='black'
            ),
            plot_bgcolor='rgba(255,255,255,0.8)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='black')
        )
        st.plotly_chart(fig)

        # Display candlestick chart and RSI
        ols_summary, candlestick_fig, rsi_fig, bb_fig, bs_fig, ma_fig = perform_ols_and_plot(stock_code)
        st.subheader("OLS Regression Results")
        st.text(ols_summary)

        st.plotly_chart(candlestick_fig)
        st.plotly_chart(rsi_fig)
        st.plotly_chart(bb_fig)
        st.plotly_chart(bs_fig)
        st.plotly_chart(ma_fig)



    else:
        st.error(f"Không tìm thấy thông tin giá cho cổ phiếu {stock_code}.")

# Display stock news information
def display_stock_news(stock_code):
    stock_code = stock_code.upper()
    news_data = get_news_from_csv(stock_code)

    if not news_data.empty:

        items_per_page = 5
        total_pages = len(news_data) // items_per_page + (1 if len(news_data) % items_per_page > 0 else 0)

        current_page = st.number_input('Chọn trang', min_value=1, max_value=total_pages, value=1, step=1)
        st.subheader(f"Tin tức liên quan cho cổ phiếu {stock_code}: (Trang {current_page}/{total_pages})")

        start_index = (current_page - 1) * items_per_page
        end_index = start_index + items_per_page
        paginated_news = news_data.iloc[start_index:end_index]

        for _, news_item in paginated_news.iterrows():
            st.markdown(f'<div class="news-date">{news_item["Time"]}</div>', unsafe_allow_html=True)
            st.markdown(f'<a href="{news_item["Link"]}" class="news-title">{news_item["Title"]}</a>', unsafe_allow_html=True)
            st.markdown('<hr>', unsafe_allow_html=True)
    else:
        st.warning(f'Không tìm thấy tin tức liên quan cho cổ phiếu {stock_code}.')


# Display stock quarter report information
def display_stock_quarter_report(stock_code):
    stock_code = stock_code.upper()
    quarter_report_data = get_quarter_report_from_csv(stock_code)

    if quarter_report_data is not None:
        st.subheader(f"Báo cáo tài chính theo từng quý cho cổ phiếu {stock_code}: ")
        unique_times = quarter_report_data['Time'].unique()
        selected_time = st.selectbox('Chọn mốc thời gian', unique_times)
        filtered_data = quarter_report_data[quarter_report_data['Time'] == selected_time]
        st.dataframe(filtered_data.transpose(), use_container_width=True)  # Transpose the dataframe to display vertically
    else:
        st.error(f"Không tìm thấy báo cáo tài chính theo từng quý cho cổ phiếu {stock_code}.")
        st.write(
            f"Debug info: File path tried - {os.path.join('D:\\Study Program\\Project\\Quarter_report', f'{stock_code}_quarter_report.csv')}")


# Function to read the filtered sorted stocks from the uploaded file
def get_filtered_sorted_stocks(file_path):
    try:
        df = pd.read_csv(file_path)
        df.index = range(1, len(df) + 1)
        return df
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        return pd.DataFrame()


# Display the filtered sorted stocks
def display_filtered_sorted_stocks():
    file_path = 'D:\\Study Program\\Project\\ChooseStock\\goodfindexstock.csv'
    filtered_sorted_stocks = get_filtered_sorted_stocks(file_path)

    if not filtered_sorted_stocks.empty:
        st.subheader("Các cổ phiếu nên đầu tư")
        st.dataframe(filtered_sorted_stocks)
    else:
        st.warning("Không tìm thấy dữ liệu cổ phiếu nên đầu tư.")


def perform_clustering():
    df = pd.read_csv("D:\\Study Program\\Project\\Clustering\\Clustering.csv")
    df = pd.DataFrame(df)

    # Standardize the data
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df[['Average_Percentage_Increase', 'Price_Std_dev']])

    # Determine the optimal number of clusters using the Elbow Method and Silhouette Score
    wcss = []
    silhouette_scores = []
    K_range = range(2, 10)

    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=0)
        kmeans.fit(df_scaled)
        wcss.append(kmeans.inertia_)
        silhouette_scores.append(silhouette_score(df_scaled, kmeans.labels_))

    # Plotting the Elbow Method and Silhouette Score
    plt.figure(figsize=(12, 6))

    plt.subplot(1, 2, 1)
    plt.plot(K_range, wcss, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('WCSS')
    plt.title('Elbow Method')

    plt.subplot(1, 2, 2)
    plt.plot(K_range, silhouette_scores, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('Silhouette Score')
    plt.title('Silhouette Score Method')

    plt.tight_layout()
    st.pyplot(plt)

    # Choosing the number of clusters (let's go with 3 as per the previous analysis)
    optimal_k = 6
    kmeans = KMeans(n_clusters=optimal_k, random_state=0)
    df['Cluster'] = kmeans.fit_predict(df_scaled)

    # Plotting the final clusters with stock names
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(df['Average_Percentage_Increase'], df['Price_Std_dev'], c=df['Cluster'], cmap='viridis')
    plt.xlabel('Average Percentage Increase')
    plt.ylabel('Price Std Dev')
    plt.title('KMeans Clustering of Stocks')
    plt.colorbar(label='Cluster')

    # Annotate each point with the stock name
    for i, txt in enumerate(df['Stock']):
        plt.annotate(txt, (df['Average_Percentage_Increase'][i], df['Price_Std_dev'][i]))

    plt.grid(True)
    st.pyplot(plt)


# Main content
st.title('THÔNG TIN CỔ PHIẾU VN30')

# Add an option to display the filtered sorted stocks in the sidebar
show_stocks = st.sidebar.checkbox('Hiển thị các cổ phiếu nên đầu tư')

if show_stocks:
    display_filtered_sorted_stocks()

# Sidebar for stock search
st.sidebar.header('Tìm kiếm cổ phiếu')
search_query = st.sidebar.text_input('Nhập mã cổ phiếu: ')

if search_query:
    tabs = st.tabs(['Thông tin giá', 'Tin tức', 'Báo cáo tài chính', 'Clustering'])

    with tabs[0]:
        display_stock_price_info(search_query)
    with tabs[1]:
        display_stock_news(search_query)
    with tabs[2]:
        display_stock_quarter_report(search_query)
    with tabs[3]:
        perform_clustering()

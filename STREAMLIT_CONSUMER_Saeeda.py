import json
import pandas as pd
import streamlit as st
import altair as alt
from kafka import KafkaConsumer
from PollResponseAPI import PollResponseAPI

# Custom title and subtitle with larger font sizes
st.markdown("<h1 style='font-size:36px;'>Live Poll Results</h1>", unsafe_allow_html=True)
st.markdown("<h2 style='font-size:24px;'>Real-time Poll Results Dashboard</h2>", unsafe_allow_html=True)

# Initialize metric for total responses with custom styling
total_responses = 0
st_metric = st.empty()
st_metric.markdown(f"<h3 style='font-size:20px;'>Total Responses Received: {total_responses}</h3>", unsafe_allow_html=True)

# Placeholder for question data and bar charts with Altair
question_data = {i: {} for i in range(len(PollResponseAPI.survey_questions))}
chart_containers = [st.empty() for _ in PollResponseAPI.survey_questions]

# DataFrame for displaying responses
responses_df = pd.DataFrame(columns=['Answer ID', 'Q1', 'Q2', 'Q3'])
responses_table = st.empty()

# Footer with styling
st.markdown("<p style='font-size:14px;'>App Created by Saeeda Ramzan</p>", unsafe_allow_html=True)

# Set up Kafka Consumer
consumer = KafkaConsumer(
    'live_poll_responses',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Process incoming messages from Kafka
for message in consumer:
    poll_response = message.value
    
    # Update total responses
    total_responses += 1
    st_metric.markdown(f"<h3 style='font-size:20px;'>Total Responses Received: {total_responses}</h3>", unsafe_allow_html=True)
    
    # Extract and update response data
    answer_id = poll_response['answer_id']
    answers = poll_response['answer_array']
    
    # Update question data for bar charts
    for i, answer in enumerate(answers):
        question_text = list(answer.keys())[0]
        response_value = answer[question_text]
        
        # Update response counts
        if response_value in question_data[i]:
            question_data[i][response_value] += 1
        else:
            question_data[i][response_value] = 1

        # Prepare data for Altair chart
        chart_data = pd.DataFrame({
            'Response': list(question_data[i].keys()),
            'Count': list(question_data[i].values())
        })
        
        # Create Altair bar chart with customized label fonts
        chart = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Response:N', title=question_text, axis=alt.Axis(labelFontSize=14, titleFontSize=16)),
            y=alt.Y('Count:Q', title='Count', axis=alt.Axis(labelFontSize=14, titleFontSize=16))
        ).properties(
            width=600,
            height=300,
            title=alt.TitleParams(question_text, fontSize=20)
        )

        # Display the updated bar chart
        chart_containers[i].altair_chart(chart, use_container_width=True)
    
    # Update DataFrame with new response
    new_row = [answer_id] + [list(ans.values())[0] for ans in answers]
    responses_df.loc[len(responses_df)] = new_row
    responses_table.write(responses_df)

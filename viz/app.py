import os

import flask

import pandas as pd
import plotly.graph_objs as go
import plotly
import plotly.tools as tls

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from pymongo import MongoClient
from matplotlib import pyplot as plt

from conf import db_conf

def get_count_scores():
    aux_dict = {}
    result_dict = {}
    pipeline_neg = [{"$match": {"sentiment_score":{"$lt":0}}}, {"$group":{"_id":"$submission_id", "negative_scores":{"$sum":1}}}]
    pipeline_pos = [{"$match": {"sentiment_score":{"$gt":0}}}, {"$group":{"_id":"$submission_id", "positive_scores":{"$sum":1}}}]
    pipeline_neu = [{"$match": {"sentiment_score":{"$eq":0}}}, {"$group":{"_id":"$submission_id", "neutral_scores":{"$sum":1}}}]
    neg_entries = list(comments.aggregate(pipeline_neg))
    pos_entries = list(comments.aggregate(pipeline_pos))
    neu_entries = list(comments.aggregate(pipeline_neu))
    for entry in neg_entries:
        aux_dict[entry["_id"]] = {
            "positive":0,
            "negative":entry["negative_scores"],
            "neutral":0,
        }
    for entry in pos_entries:
        try:
            aux_dict[entry["_id"]]["positive"] = entry["positive_scores"]
        except:
            aux_dict[entry["_id"]] = {
                "positive":entry["positive_scores"],
                "negative":0,
                "neutral":0,
            }
    for entry in neu_entries:
        try:
            aux_dict[entry["_id"]]["neutral"] = entry["neutral_scores"]
        except:
            aux_dict[entry["_id"]] = {
                "positive":0,
                "negative":0,
                "neutral":entry["neutral_scores"],
            }
    subs = list(submissions.find({"id" : {"$in":list(aux_dict.keys())}}))
    for sub in subs:
        positive = aux_dict[sub["id"]]["positive"]
        negative = aux_dict[sub["id"]]["negative"]
        neutral = aux_dict[sub["id"]]["neutral"]
        try:
            result_dict[sub["topic"]]["values"][0] += positive
            result_dict[sub["topic"]]["values"][1] += negative
            result_dict[sub["topic"]]["values"][2] += neutral
        except:
            result_dict[sub["topic"]] = {
               "labels": ["positive", "negative", "neutral"],
               "values": [positive, negative, neutral]
            }
    return result_dict


def get_rolling_mean(topic):
    all_comments = list(comments.find({}))
    all_ids = list(set([x["submission_id"] for x in all_comments]))
    all_submissions = list(submissions.find({"id":{"$in":all_ids}}))
    com_df = pd.DataFrame.from_dict(all_comments)[["sentiment_score", "sentiment_magnitude", "submission_id", "date"]]
    subs_df = pd.DataFrame.from_dict(all_submissions)[["id", "topic"]]
    subs_df = subs_df[subs_df.topic == int(topic)]
    merged_df = com_df.merge(subs_df, left_on="submission_id", right_on="id")[["sentiment_score", "sentiment_magnitude", "topic", "date"]]
    merged_df['date'] = pd.to_datetime(merged_df.date)
    merged_df.set_index("date", inplace=True)
    merged_df.sort_index(inplace=True)
    resulting_df = merged_df[["sentiment_score", "sentiment_magnitude"]].rolling('15s').mean()
    return resulting_df

app = dash.Dash(__name__)

mongo_client = MongoClient(db_conf["MONGO_CONNECTION_STRING"])
db = mongo_client.get_database("reddit-stream")
topics = db['topics']
comments = db['comments']
submissions = db['submissions']


app.css.append_css(
    {'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'}
)
static_image_route = "/static/"
image_directory = os.path.abspath("static/img")

all_topics = list(topics.find({},{"_id":0}))
topics_options = [
    {
        'label':"Topic {}: ".format(topic['topic'])+", ".join(list(topic['keywords'].keys())[:3]), 
        "value": topic['topic']
    } for topic in all_topics
]


app.layout = html.Div(
    [
        html.Div(
            [
                html.H1(
                    'Reddit r/worldnews Sentiment Analysis and Topic Modelling',
                    className='eight columns',
                ),
                html.Img(
                    src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe.png",
                    className='one columns',
                    style={
                        'height': '100',
                        'width': '225',
                        'float': 'right',
                        'position': 'relative',
                    },
                ),
            ],
            className='row'
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P('Select Topic'),
                        dcc.Dropdown(
                            id='select_topic',
                            options=topics_options,
                            multi=False,
                            value=all_topics[0]['topic'],
                        ),
                    ],
                    className='six columns'
                ),
            ],
            className='row'
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(id='wordcloud', style={'width': '600px', 'height':'400px'})
                    ],
                    className='seven columns',
                    style={'margin-top': '20'}
                ),
                html.Div(
                    [
                        dcc.Graph(id='sentiment_pie')
                    ],
                    className='five columns',
                    style={'margin-top': '20'}
                )
            ],
            className='row'
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Graph(id='submissions_graph'),
                        dcc.Interval(
                            id='interval-component',
                            interval=5*1000, # in milliseconds
                            n_intervals=0
                        )
                    ],
                    className='twelve columns',
                    style={'margin-top': '10'}
                ),

            ],
            className='row'
        ),
    ],
    className = 'ten columns offset-by-one'
)


# Word Cloud
@app.callback(Output('wordcloud', 'src'),
              [Input('select_topic', 'value')])
def make_word_cloud(topic):
    filename="topic_{}_worldcloud.png".format(topic)    
    return static_image_route + filename

# Sentiment Pie
@app.callback(Output('sentiment_pie', 'figure'),
              [Input('select_topic', 'value')])
def make_sentiment_pie(topic):
    try:
        result = get_count_scores()[int(topic)]
        labels = result["labels"]
        values = result["values"]
    except:
        labels = ["positive", "negative", "neutral"]
        values = [0, 0, 0]
    colors=['#0000FF', '#FF0000', '#D3D3D3']
    trace = go.Pie(labels = labels, values = values,
        hoverinfo = 'label+percent', textinfo = 'value',
        textfont = dict(size=20),
        marker = dict(colors=colors,
        line=dict(color='#000000', width=2)))
    data=go.Data([trace])
    layout=go.Layout(title = "Sentiment {}".format(topic))
    figure = go.Figure(data = data, layout = layout)
    return figure

@app.callback(Output('submissions_graph', 'figure'),
             [Input('select_topic', 'value'),
             Input('interval-component', 'n_intervals')])
def make_submissions_pie(topic, n):
    df = get_rolling_mean(topic)
    trace_sentiment = go.Scatter(
        x=df.index,
        y=df['sentiment_score'],
        name = "sentiment_score",
    )
    trace_magnitude = go.Scatter(
        x=df.index,
        y=df['sentiment_magnitude'],
        name = "sentiment_magnitude",
        opacity = 0.8
    )
    data = [trace_sentiment, trace_magnitude]

    layout = dict(
        title='Rolling Mean (15s)',
        xaxis=dict(type='date')
    )
    fig = dict(data=data, layout=layout)
    return fig

@app.server.route('{}<image_path>.png'.format(static_image_route))
def serve_image(image_path):
    image_name='{}.png'.format(image_path)
    return flask.send_from_directory(image_directory, image_name)

if __name__ == '__main__':
    app.server.run(debug = True, threaded = True)

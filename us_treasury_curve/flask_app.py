from flask import Flask, render_template, Response
import plotly.io as pio
from plotter import generate_interest_rate_plot

app = Flask(__name__)

@app.route('/')
def home():
    fig = generate_interest_rate_plot()
    graph_html = pio.to_html(fig, full_html=False)
    return render_template('index.html', plot=graph_html)

@app.route('/chart')
def chart():
    fig = generate_interest_rate_plot()
    graph_html = pio.to_html(fig, full_html=True)
    return Response(graph_html, mimetype='text/html')

if __name__ == '__main__':
    app.run(debug=True)


# <iframe src="http://127.0.0.1:5000/chart" width="100%" height="600px" frameborder="0"></iframe>

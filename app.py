from flask import Flask, render_template, url_for, request, redirect
from search import DbManager
import os.path


app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_manager = DbManager(os.path.join(BASE_DIR, "LOVECORP.db"))


@app.route('/')
@app.route('/home')
def index():
    return render_template('index.html')


@app.route('/search', defaults={'query': None}, methods=['GET', 'POST'])
@app.route('/search/<query>', methods=['GET', 'POST'])
def search(query: str):
    if request.method == 'POST':
        data = request.form['query']
        return redirect(url_for('search', query=data))

    if not query:
        return render_template('create.html', total=0)

    result = db_manager.search(query)
    return render_template('search.html', items=result, total=len(result))

@app.route('/about')
def about():
    return render_template('about.html')


if __name__ == '__main__':
    app.run(debug=True)

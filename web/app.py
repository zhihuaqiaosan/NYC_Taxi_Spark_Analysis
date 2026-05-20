from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data/<filename>')
def get_data(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    print("🚀 可视化仪表板启动中...")
    print("📊 打开浏览器访问: http://localhost:5000")
    app.run(debug=True, port=5000)

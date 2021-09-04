from flask import Flask, render_template, request
import random, copy
import json

app = Flask(__name__)

with open("questions.json","r") as file:
    original_questions = json.load(file)

questions = copy.deepcopy(original_questions)


@app.route('/')
def quiz():
 for i in questions.keys():
  random.shuffle(questions[i])
 return render_template('index.html', q = questions, o = questions)


@app.route('/quiz', methods=['POST'])
def quiz_answers():
 correct = 0
 for i in questions.keys():
  answered = request.form[i]
  if original_questions[i][0] == answered:
   correct = correct+1
 return '<h1>Correct Answers: <u>'+str(correct)+'</u></h1>'

if __name__ == '__main__':
 app.run(debug=True)
from flask import Flask,session,redirect,url_for,request

import os
os.urandom(24)


app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)

@app.route("/")
def index():
    if session.get('username')=='shiyanlou' and session.get('password')=='shiyanlou':
        return "hello shiyanlou"
    return "you are not logged in"

@app.route("/login",methods=["POST","GET"])
def login():
    if request.method=='POST':
        session['username']=request.form['username']
        session['password']=request.form['password']
        return redirect(url_for('index'))
    return """
    <form action="" method="post">
        <p><input type=text name=username>
        <p><input type=text name=password>
        <p><input type=submit value=Login>
    </form>
    """

@app.route("/logout")
def logout():
    session.pop('username',None)
    session.pop('password',None)
    return redirect(url_for('index'))

if __name__=="__main__":
    app.run(debug=True)

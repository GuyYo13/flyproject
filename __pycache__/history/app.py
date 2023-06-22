from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Retrieve input from the form
        input_data = request.form.get('input_data')
        
        # Process the input data
        result = process_input(input_data)
        
        # Render the template with the result
        return render_template('result.html', result=result)
    
    # If it's a GET request, simply render the form
    return render_template('form.html')

def process_input(input_data):
    # Perform your Python script's logic here
    # You can modify this function to suit your specific script's requirements
    processed_data = input_data.upper()
    
    return processed_data

if __name__ == '__main__':
    app.run(debug=True)

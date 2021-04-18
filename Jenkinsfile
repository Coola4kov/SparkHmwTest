pipeline {
    agent any

    stages {
        stage('Test') {
            steps {
                echo 'Testing..'
                withPythonEnv('python3') {
                    sh 'pip install pytest'
                    sh 'pytest'
                }
            }
        }
        stage('Build') {
            steps {
                echo 'Building..'
                withPythonEnv('python3') {
                    sh 'pip install poetry==1.1.4 build==0.2.1'
                    sh 'python3 -m build'
                }
            }
        }

    }
}

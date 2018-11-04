pipeline {
    agent {
        docker { image 'toshisearch/builder:latest' }
    }

    stages {
        stage('Build Development') {
            steps {
                sh "cargo build"
            }
        }
        stage('Test') {
            steps {
                sh "cargo test"
            }
        }
    }
}
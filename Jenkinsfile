pipeline {
  agent {
    docker {
      image 'rust:latest'
    }

  }
  stages {
    stage('Install Capn Proto') {
      steps {
        sh 'sudo apt-get -y install capnproto'
      }
    }
    stage('Build Development') {
      steps {
        sh 'cargo build'
      }
    }
    stage('Test') {
      steps {
        sh 'cargo test'
      }
    }
    stage('Clippy') {
      steps {
        sh 'cargo clippy --all'
      }
    }
    stage('Rustfmt') {
      steps {
        sh 'cargo fmt --all -- --write-mode diff'
      }
    }
  }
}
pipeline {
    agent {
        dockerfile {
            filename 'Dockerfile.jenkins'
            args '-u root:root --cap-add SYS_PTRACE -v "/tmp/gomod":/go/pkg/mod'
            label 'main'
        }
    }
    stages {
        stage('Download') {
            steps {
                checkout scm
                sh 'go mod download'
            }
        }
        stage('Test') {
            steps {
                sh 'go test ./...'
            }
        }
    }
}
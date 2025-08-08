# Setting Up A Local Development Environment

* Installing **git**
    * [1.5 Getting Started - Installing Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) 
* Authenticating into **GitHub**
    * I usually use an [ssh key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh).
    * If that is a pain, [use the gh cli](https://cli.github.com/).
    * This seems like overkill, but you can also use [GitHub Desktop](https://docs.github.com/en/desktop/installing-and-authenticating-to-github-desktop/authenticating-to-github-in-github-desktop).
* Installing the **Google Cloud CLI:**
    * [gcloud CLI overview](https://cloud.google.com/sdk/gcloud) 
        * Authenticate in with:  `gcloud auth`
        * Check current project:` gcloud config get-value project`
        * Set project:<code> gcloud config set project <em>&lt;PROJECT_ID></em></code>
* Installing and configuring <strong>vscode</strong>
    * [Visual Studio Code](https://code.visualstudio.com/)
    * Install <em>Dataform</em> and GitLens extensions:  
* Installing <strong>node </strong>and <strong>npm</strong>
    * [Installing Node.js and npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm)
    * <strong><em><span style="text-decoration:underline;">USE A NODE VERSION MANAGER!</span></em></strong>
* Installing <strong>dataform</strong>:
    * [Dataform CLI](https://cloud.google.com/dataform/docs/use-dataform-cli#install-dataform-cli)
* Cloning a <strong>GitHub repository:</strong>
    * <code>git clone [git@github.com](mailto:git@github.com):BioNewsInc/dataform-warehouse.git</code>
* Running the <strong>Dataform repo:</strong>
    * <code>cd dataform-warehouse/dataform</code>
    * <code>npm install</code>
    * <code>dataform init-creds bigquery</code>
    * <code>dataform compile</code>
    * <code>dataform run --dry-run</code>
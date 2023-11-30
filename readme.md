# Univ3 Substreams Downloader

This tool allows you to quickly query historical swap data from univ2 and univ3 subgraphs.

## Setup

After cloning the repository, create a virtual environment and install `subgrounds`
### Virtual Environment

1. Install virtual environment:

    ```bash
    python3 -m venv .venv
    ```

2. Activate virtual environment:

    ```bash
    source .venv/bin/activate
    ```

3. Install Subgrounds:

    ```bash
    pip install subgrounds
    ```


### Playgrounds API Key

1. Sign up for a playgrounds API key to get 5000 free queries [here](https://docs.playgrounds.network/api/key/).

2. Load the API key into the local environment variable using bash.

    #### For Bash:

    - Open or create your Bash configuration file (e.g., `~/.bashrc` or `~/.bash_profile`) using a text editor.

        ```bash
        nano ~/.bashrc
        ```

    - Add the following line at the end of the file, replacing "your_api_key_here" with your actual API key:

        ```bash
        export PLAYGROUNDS_API_KEY="your_api_key_here"
        ```

    - Save the file and exit the text editor.

    - Apply the changes immediately:

        ```bash
        source ~/.bashrc
        ```

## Usage

Run the following commands to query the subgraphs using the format `python univ3_substreams_async.py <date> <days>`. For example the below examples will query the subgraphs swap entity for the 7 days following 2023-11-20:

`python univ3_substreams_async.py 2023-11-20 7` -> Query for 2023-11-26 completed in 74.38s

`python univ2_async.py 2023-11-20 7` -> 10931 queries...
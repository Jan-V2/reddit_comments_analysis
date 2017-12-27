data_1_name="stage_1_dirty.db"
data_2_name="stage_2_clean.db"
out_file_name="result.db"
data_path_net="s3://rediitdatacleaning"
repo_url="https://github.com/johnvanderholt/reddit_comments_analysis"
script="clean_bodies.py"
apt-get update
wait
apt-get --assume-yes install python3 python3-pip
wait
pip3 install -r requirements.txt
wait
git clone $repo_url $PWD/temp
wait
python3 -m awscli s3 cp $data_path_net/$data_1_name $PWD/$data_1_name
wait
python3 -m awscli s3 cp $data_path_net/$data_2_name $PWD/$data_2_name
wait
python3 $PWD/temp/$script $PWD/$data_1_name $PWD/$data_2_name
wait
python3 -m awscli s3 cp $PWD/$data_2_name $data_path_net/$out_file_name
wait
shutdown
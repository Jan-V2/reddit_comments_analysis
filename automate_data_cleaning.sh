data_1_name = "dirty.db"
data_2_name = "clean.db"
out_file_name = $data_2_name
data_path_net = "test"
repo_url = "test"
script = "test"

sudo apt install python3, python3-pip -y
sudo pip3 install -r requirements.txt

git clone $repo_url $PWD/temp
python3 -m awscli s3 cp $data_path_net/$data_1_name $PWD/$data_1_name
python3 -m awscli s3 cp $data_path_net/$data_2_name $PWD/$data_2_name

python3 $PWD/temp/$script $PWD/$data_1_name $PWD/$data_2_name

python3 -m awscli s3 cp $PWD/$out_file_name $data_path_net/$out_file_name
sudo shutdown
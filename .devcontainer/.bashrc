cat /workspace/.devcontainer/welcome.txt
echo "Fish is your friend!!!"
echo "TYPE ""fish"""
alias stopall="docker container stop $(docker container ls -aq)"
alias removeall="ocker container rm $(docker container ls -aq)"
 

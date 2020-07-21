function setup
{
case `uname` in
  Linux )
     LINUX=1
     which dnf && { echo fedora; sudo dnf install python3; pip3 install -r requeriments.txt; return; }
     which zypper && { echo opensuse; sudo zypper install python3; pip3 install -r requeriments.txt; return; }
     which apt && {  echo debian; sudo apt install python3; pip3 install -r requeriments.txt; return; }
     ;;
  Darwin )
     DARWIN=1
     brew install python3 ; pip3 install -r requeriments.txt;
     ;;
  * )
     # Handle AmgiaOS, CPM, and modified cable modems here.
     ;;
esac
} 


setup
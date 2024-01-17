一个简单的HTTP长轮询的例子，其实就是利用了keep-alive 并且不返回长度的原理，可以通过抓包查看具体的通讯过程
sudo tcpdump -i lo0 host 127.0.0.1 and port 8080  -X -e -v -n -vv
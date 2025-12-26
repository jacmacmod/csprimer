import urllib.request


def fetch(url):
    with urllib.request.urlopen(url) as response:
        content = response.read().decode('utf-8')
        return content

def fib(n):
    if n <= 1:
        return n
    return fib(n - 1) + fib(n - 2)

def cacher(f):
    cache = {}
    print("here")
    def g(arg):
        print("cache id:", id(cache), "arg:", arg)   # 👈 same id every time
        if arg in cache:
            print("HIT", arg)
            return cache[arg]
        res = f(arg)
        cache[arg] = res
        return cache[arg]
    return g

if __name__ == '__main__':
    # gc = cacher(fetch)
    # gc('http://google.com')
    # gc('http://google.com')
    # fib = cacher(fib)
    print(cacher(fib)(5))
    fib = cacher(fib)
    print(fib)
    print(fib(5))
    
    
    # print(fetch('http://google.com')[:80])
    

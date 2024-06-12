import requests
import json
import os

url = 'https://shopee.vn/api/v4/pages/get_category_tree'

header = {
    'accept': '*/*',
    'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
    'af-ac-enc-dat': 'd136b88a3da1e25f',
    'af-ac-enc-sz-token': 'p2ogjl2DIYdrzwZUsf/RJQ==|LndUr3T4vEQJYwwIDCyIIb2xmrDFuhuqaLiSYvR7uTWHMuXj7fRsWxN0Cr1Mcx4B4fg+TxkxQ3/hKQ==|w2+4Rygct13G4H7Y|08|3',
    'cookie': 'SPC_F=mHM66K2jqvnwkV37MHXQwK9FMUsAv1dI; REC_T_ID=9f76e016-7f0c-11ee-9303-626fdfb49834; _ga_M32T05RVZT=GS1.1.1699540268.1.1.1699540685.59.0.0; _gcl_au=1.1.826404875.1713520988; _fbp=fb.1.1713520988135.499476239; SPC_CLIENTID=bUhNNjZLMmpxdm53ysmlguujiwjdjrqd; _hjSessionUser_868286=eyJpZCI6IjMwNzA0ZTA0LTI1MDctNTk1OC05MDZjLTkxYTg5Y2U4NmI0YyIsImNyZWF0ZWQiOjE2OTk1NDAyNzAyODUsImV4aXN0aW5nIjp0cnVlfQ==; SC_DFP=dAqgYfCYAsHhpjWMbhAAHpnBEhyQHVjb; _ga_3XVGTY3603=GS1.1.1716019327.3.0.1716019333.54.0.0; SPC_U=546859712; SPC_R_T_IV=Vjd2ZHE1cUtwRFRtZ1RsMQ==; SPC_T_ID=UjO3aJ0ew/qdT/DvpfosMzUnTf7HcAJTZTgq3aKZeuj0XLeuF7XerfHPLa925PwekwwtB+1whSYHUqKL3SUSoQ07SvIK1Aruf2T1vaBShgz0U/junNJ2duNRnyHBKrKIhBgWjAMab3Kl7AcbFWhHxyXugnZsuAaY7ulwgwkX9iE=; SPC_T_IV=Vjd2ZHE1cUtwRFRtZ1RsMQ==; SPC_R_T_ID=UjO3aJ0ew/qdT/DvpfosMzUnTf7HcAJTZTgq3aKZeuj0XLeuF7XerfHPLa925PwekwwtB+1whSYHUqKL3SUSoQ07SvIK1Aruf2T1vaBShgz0U/junNJ2duNRnyHBKrKIhBgWjAMab3Kl7AcbFWhHxyXugnZsuAaY7ulwgwkX9iE=; SPC_SI=Qr04ZgAAAABnSW1QVjRWM/09MQcAAAAAV3NIeWk5Uk8=; _gid=GA1.2.1586736082.1717806399; _med=cpc; SPC_EC=.NkZWMGNWOGdPYkVIQm8zYZiekrT24+TtfDAspHABurE0w4Pg+9F6uET9sixkqnXsevm6ctI2uIevspPZt5mdjEInsq6CKdvgiTawDSTlllUmmOJpM+eAmdT3e+pdVVSMD9WefMhqamSosC8yktsOyDLXXDs21pGU/8RBeSnm7IbYxuIpCb0RF035kaRVGw1M9RYgz3n7cx1itsCBUV+F0A==; SPC_ST=.NkZWMGNWOGdPYkVIQm8zYZiekrT24+TtfDAspHABurE0w4Pg+9F6uET9sixkqnXsevm6ctI2uIevspPZt5mdjEInsq6CKdvgiTawDSTlllUmmOJpM+eAmdT3e+pdVVSMD9WefMhqamSosC8yktsOyDLXXDs21pGU/8RBeSnm7IbYxuIpCb0RF035kaRVGw1M9RYgz3n7cx1itsCBUV+F0A==; _gac_UA-61914164-6=1.1717933315.Cj0KCQjwpZWzBhC0ARIsACvjWRPMv9DoX_y6RTMeHUCHBPfqj4jDgoAXrHO3Cx3iwxZVfLwOePUwx3QaAqGzEALw_wcB; REC7iLP4Q=ec7f2fdd-7e0f-4f85-82c1-5a669ab93f1a; _gcl_aw=GCL.1718073186.CjwKCAjwtqmwBhBVEiwAL-WAYVDGQ3EsTE8CK78vvNfv7ikCyH5lptp6cVnozWxJvQ3cMDyLuvlmFBoC3bIQAvD_BwE; _gcl_gs=2.1.k1$i1718073185; SPC_SEC_SI=v1-UkppRzg3TnplM1lOcjBUbQmMFZXuZ67Up9M7as9EqFmyVuRCpc7W2jR/joJgNrItWKRb3hg+0DR9FwFdpmVQwV8dD363Izo+IPui3arF3QE=; __LOCALE__null=VN; csrftoken=M89SORlefixDoQcT40Er0J51yCoixSaj; _QPWSDCXHZQA=0c3fdbb4-9a39-410f-eb93-77d56a74ffc1; _sapid=9ee3068bc8a224ed60cd816fb95b090b0e2e4b0a1ac8e88b6137c8bb; SPC_IA=1; SPC_CDS_CHAT=fc72466d-22f2-4c9b-949f-ca38ef69aa48; _hjSession_868286=eyJpZCI6IjY2YzcyMGIzLWFmOGUtNDg2OC1iZjNmLWRkM2RmNDQ0ZjM3MiIsImMiOjE3MTgxODgyNDcwMjYsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; AMP_TOKEN=%24NOT_FOUND; _ga=GA1.2.709134354.1699540269; shopee_webUnique_ccd=p2ogjl2DIYdrzwZUsf%2FRJQ%3D%3D%7CLndUr3T4vEQJYwwIDCyIIb2xmrDFuhuqaLiSYvR7uTWHMuXj7fRsWxN0Cr1Mcx4B4fg%2BTxkxQ3%2FhKQ%3D%3D%7Cw2%2B4Rygct13G4H7Y%7C08%7C3; ds=c54de136da5120bb7e204220716475cd; _ga_4GPP1ZXG63=GS1.1.1718188247.50.1.1718189154.60.0.0',
    'if-none-match-': '55b03-158a0f98f80a09649f8dcfd2f8d263d4',
    'priority': 'u=1, i',
    'referer': 'https://shopee.vn/',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'x-api-source': 'pc',
    'x-requested-with': 'XMLHttpRequest',
    'x-sap-ri': '647c69665b0b58f5da2be23a030168a51bddfa97751230c0b602',
    'x-sap-sec': 'VVtWztcR3ZFN7ZFN63F27ZmN63FN7ZmN7ZFB7ZFN7ZmN7gFB7ZFR7ZFN2idv8eJN7ZAz7eFNGZtN7lVfZxfEh+xfUs+F4hOD8qEIzK0ute3VUaLnkFOehINrykBdj2yKl0NGpvyy4deFZ1SrCxhiE8HolWyfuRUjHRhBwxyHLGquirYSdX++3w0TbT6XetV9oNnnWDWoCSs4jx+y7vEiWCB7g6EQDOFHTTKa9VjJBG8GnLAqCDJR/H7qAmc8UIVHERfMkhW5mVKaYKY1eqswPQiV3H8OYSMRyrRnCozfloIaXlajlQ8xCuJ8HmHlz7yprbrOGnbuQReWOIRNAoPCswF+VnlJfj9dtqA12JqM8Ade21R3aAtxd5ig/U/9dnGthz+HEFhTNCD3NGqikcfDSCL7SSp0znLp2WJ5ZXR+CnhEeYimHbDQAGfrk7PsWv4Aulg2+xEw3J2HeMfEvWQkU0J2VP7o3W0CXjxGrvP4Z6pUH4oUe8cAdYFAVf/IKVfCLbPC0IcUl7AIUXv549aIuMEOq4c9/kuH821lomOgnknaJpur36Lk+tHKrnu5+cPi2B6DXyvlyo1pWYLUSxHsEEweRNeISDEbiv67G/eTvyFwSPBvemgRkKaBITBtnGp7X87xs1xmF6HvtWTIqes8Tc0gqD8m1H2IeCZgeopi0/9z+ppLz+YAEB+SGaeiq1JMMPmpfjNb4YUJTwJDGvxAPNTYAq+8+nWh4ajAd9COstB3QoWgROV9wR2vnRMkvRmSdjb4vhwPuIPMvHSHyJ1/puFLwnlo2QSKEpfvK4JV8TEZhdSnXYz8cD5+/wUsEBotvwsHyipB9ODdFxx4z3DInWA6k5kgwbgGMdaKlt/Z6lZ0X6s5flgeKPa27PWi30b2yrAdxLh40M90Y7fJiect6tBTP21sxgEqUED14P2xt/jO0B5R67/O9GEVJ+wI3485HyWJqdXt8b0qHbDQ2LG5TVocACvwD/Kf6HZDTSriHjNRXWIyKq/0lGyV+Z7HVMAh9reMZPvNJljZ6Ql3VWnhYqLSATsvUFzc6I+vjb2n/5rQ2A6Ew3wq/mVQP04v1bU5W/eIg4nd/nPyZxgz+G+6+fnmT4eO1cJePJ2PpKHweZOzFs0qKffgTmIIYxW7x7rJTlyXI+vmsX6IfQuqt7s7PaX5wDFoA7OMpAxzbWMoF2D8J31daPv46V9PqU1orTZB15jLrewhGAS4/Gt0jjHvr1UKKfhftZWCqrBE9tfawSqIWCSd2Sm3StJ7liZ1ZmA2XH/nbrXcqnbbTtcTP3w1jykrUodW2ZGN7ZF8BwUWNSgu7gdv8kL27ZFNLZFN7xmN7ZTSkEE5Tr3zW//ZBQ5CUHWSGkZd4eJN7ZDg+SGANuZXNUFN7ZF27ZcN/ZFR7ZJN7ZF27ZFNLZFN7xmN7ZAQfeFqzJqIGT3RNxg7m4Z3O6qRl3JN7ZF85unmNSm8+3FN7ZD=',
    'x-shopee-language': 'vi',
    'x-sz-sdk-version': '1.10.1'
}

c = requests.get(url = url, headers = header).content.decode('utf-8')
tree = json.loads(c)['data']['category_list']

f = open("savedata.json", "w")
f.write(c)
f.close()

os.makedirs("data123")
for i in range (len(tree)):
    tmp = "data123/" + str(tree[i]['catid']) + "/"
    os.makedirs(tmp)
    for j in range(len(tree[i]['children'])):
        os.makedirs(tmp + str(tree[i]['children'][j]['catid']))
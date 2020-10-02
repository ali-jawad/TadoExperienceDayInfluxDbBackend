package com.tado

class UrlMappings {

    static mappings = {

        "/report"(controller: 'reportRest', action: 'showReport')

        "/"(view:"/index")
        "500"(view:'/error')
        "404"(view:'/notFound')
    }
}

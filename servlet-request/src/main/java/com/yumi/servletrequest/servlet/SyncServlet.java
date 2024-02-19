package com.yumi.servletrequest.servlet;



//import javax.servlet.annotation.WebServlet;
//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;


import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@WebServlet(urlPatterns = "/sync", name = "SyncServlet")
public class SyncServlet extends HttpServlet {

    private static final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)  {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
        String name = req.getParameter("name");
        System.out.println(Thread.currentThread().getName() + " --> " + name);

        executor.execute(() -> {
            try {
                TimeUnit.SECONDS.sleep(10);
                String threadName = req.getParameter("name");
                System.out.println(Thread.currentThread().getName() + " --> " + threadName);
            } catch (Exception e) {
                //ignore
                e.printStackTrace();
            }
        });
    }
}

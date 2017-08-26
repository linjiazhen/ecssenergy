package com.wintoo.tools;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;


import javax.mail.MessagingException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.servlet.http.HttpSession;
import java.io.File;
import java.util.Properties;

@Service
public class EmailService {
    @Autowired
    private JavaMailSender mailSender;
    @Autowired
    private JavaMailSenderImpl mailSenderAttach;

    public void sendMail(String subject,String email,String text) {
        SimpleMailMessage msg = new SimpleMailMessage();
        msg.setSubject(subject);
        msg.setTo(email); // 对方邮箱
        msg.setText(text);
        this.mailSender.send(msg);
    }

    public void sendAttachmentEmail(HttpSession session) throws MessagingException {

        MimeMessage message = this.mailSenderAttach.createMimeMessage();

        // 第二个参数设置为TRUE，即multipart=true时才能发送附件
        MimeMessageHelper helper = new MimeMessageHelper(message, true);
        // 设置发送方邮箱地址
        helper.setFrom(new InternetAddress("ljzorz@163.com"));
        // 设置接收方邮箱地址
        helper.setTo("ljzorz@vip.qq.com");
        // 发送内容
        helper.setText("有附件!");
        // 附件(假如是个图片)
        helper.addAttachment("wash.png", new FileSystemResource(new File(session.getServletContext().getRealPath("/") + "/static/images/wash.png")));
        // 发送
        this.mailSenderAttach.send(message);
    }
}



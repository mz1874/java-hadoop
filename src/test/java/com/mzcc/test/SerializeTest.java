package com.mzcc.test;

import com.mzcc.entity.Person;
import org.junit.Test;

import java.io.*;

public class SerializeTest {

//    @Test
//    public void testSerialize() throws IOException, ClassNotFoundException {
//
//        Person person = new Person();
//        person.setName("张三");
//        person.setAge(12);
//
//
//        FileOutputStream file = new FileOutputStream("E:\\1.txt");
//        ObjectOutputStream objectOutputStream = new ObjectOutputStream(file);
//        objectOutputStream.writeObject(person);
//        objectOutputStream.close();
//        file.close();
//
//
//        FileInputStream fileInputStream = new FileInputStream("E:\\1.txt");
//
//        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//
//        Person user = (Person) objectInputStream.readObject();
//
//        System.out.println(user.getName());
//        System.out.println(user.getAge());
//        objectInputStream.close();
//        fileInputStream.close();
//
//    }
}

package com.wfy.beans;

public class Job {
    private String job;
    private String experience;
    private String education;
    private double salary;
    private String company;
    private String city;

    public Job() {
    }

    public Job(String job, String experience, String education, String salary, String company, String city) {
        this.job = job;
        this.experience = experience;
        this.education = education;
        this.salary = new Double(salary);
        this.company = company;
        this.city = city;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getExperience() {
        return experience;
    }

    public void setExperience(String experience) {
        this.experience = experience;
    }

    public String getEducation() {
        return education;
    }

    public void setEducation(String education) {
        this.education = education;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary() {
        this.salary = salary;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "Job{" +
                "job='" + job + '\'' +
                ", experience='" + experience + '\'' +
                ", education='" + education + '\'' +
                ", salary=" + salary +
                ", company='" + company + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}

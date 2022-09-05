use std::collections::HashMap;

enum IpAddr{
    V4,
    V6,
}


fn route(ip_kind: IpAddr) {

}

enum Option<T> {
    None,
    Some(T),
}

enum Coin {
    Penny, 
    Nickle, 
    Dime,
}


fn largest<T>(list: &[T]) -> T {
    let mut largest = list[0];

    for &item in list {
        if item > largest {
            largest = item;
        }
    }

    largest
}


fn largest_char(list: &[char]) -> char {
    let mut largest = list[0];

    for &c in list {
        if c > largest {
            largest = c
        }
    }
    return largest;

}
fn main() {

    let char_list = vec!['m', 'a', 'q'];
    let d = largest_char(&char_list);
    println!("{}", d);

    let mut scores = HashMap::new();

    scores.insert(String::from("Blue"),10);
    scores.insert(String::from("Red"), 5);

    let number_list = vec![34, 44, 22, 44];
    let mut largest = number_list[0];

    for n in number_list {
        if n > largest {
            largest = n;
        }
    }
    println!("{}", largest);


    println!("{:?}",scores);

    let mut s = String::from("hello");
    s = s + "test";
    println!("{}",s);
    takes_ownership(&s);
    println!("{}", s);
    // println!("{}",fib(6));
    // fizzbuzz2(&mut 16);
}

fn takes_ownership(s: &String){
    println!("{}", s);
}
// 5
// 1,1,2,3,5,8
fn fib(a: i8) -> i8{

    let mut previous = 1;
    let mut value = 0;
    let mut counter = 0;
    
    while counter < a {
        counter += 1;
        let swap = previous;
        previous = value; 
        value = value + swap;
    }
    return value
}

fn fizzbuzz(i: &mut i8){
    let mut j = *i;
    // if 3 fizz, if 5 buzz, else fizzbuzz
    while j > 0 {
        if j % 3 == 0 && j % 5 == 0 {
            println!("FizzBuzz! {}", j)
        } else if j % 3 == 0 {
            println!("Fizz {}", j)
        } else if j % 5 == 0{
            println!("Buzz {}", j)
        }
        j = j - 1;
    }
}

fn fizzbuzz2(i :&mut i8) {
    let mut j = *i;
    while j > 0 {
        match (j%3, j%5) {
            (0,0) => println!("FizzBuzz! {}", j),
            (0, _) => println!("Fizz! {}", j),
            (_, 0) => println!("Buzz! {}", j),
            (_,_) => (),
        }
        j -= 1;
    }
}
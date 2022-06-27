use std::io;
use rand::Rng;
use std::cmp::Ordering;

fn main(){
    


    let x = 5;
    let x = x + 1;

    {
        let x = x * 2;
        println!("The valye of x {}", x)
    }

    println!("The value of x {}", x);

    println!("Guess the number!");

    let secret_number = rand::thread_rng().gen_range(1..101);
    println!("The Random Number is {}", secret_number);
    loop{
        println!("Please input your guess.");

        let mut guess = String::new();
    

        io::stdin()
            .read_line(&mut guess)
            .expect("failed to read line");


        let gues: u8 = match guess.trim().parse() {
            Ok(num) => num,
            Err(_) => continue,
        };
    
        match gues.cmp(&secret_number){
            Ordering::Less => println!("Too small!"),
            Ordering::Greater => println!("Too big!"),
            Ordering::Equal => {
                println!("You Win!");
                break
            }
        }
    }

}
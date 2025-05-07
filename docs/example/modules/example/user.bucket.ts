import Nesoi from '../../nesoi';

export default Nesoi.bucket('example::user')
    .model($ => ({
        id: $.int,
        name: $.string,
        age: $.int,
    }))
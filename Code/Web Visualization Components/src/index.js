import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';

import App from './App';
// const kType = {
//   SPACE_K: 1,
//   ST_K: 2,
// };

ReactDOM.render(<App />, document.getElementById('main'));


// data table
// class DataTable extends React.Component {
//   render() {
    
//   }
// }

// class Square extends React.Component {
//   constructor(props) {
//     super(props);
//     this.state = {
//       value: kType.SPACE_K,
//     };
//   }
//   render() {
//     return (
//       <button 
//         className="square"
//         onClick={() => {this.setState({value: 'X'})}}
//       >
//         {this.state.value}
//       </button>
//     );
//   }
// }

// class Board extends React.Component {
//   renderSquare(i) {
//     return <Square value={i} />;
//   }

//   render() {
//     const status = 'Next player: X';

//     return (
//       <div>
//         <div className="status">{status}</div>
//         <div className="board-row">
//           {this.renderSquare(0)}
//           {this.renderSquare(1)}
//           {this.renderSquare(2)}
//         </div>
//         <div className="board-row">
//           {this.renderSquare(3)}
//           {this.renderSquare(4)}
//           {this.renderSquare(5)}
//         </div>
//         <div className="board-row">
//           {this.renderSquare(6)}
//           {this.renderSquare(7)}
//           {this.renderSquare(8)}
//         </div>
//       </div>
//     );
//   }
// }

// class Game extends React.Component {
//   render() {
//     return (
//       <div className="game">
//         <div className="game-board">
//           <Board />
//         </div>
//         <div className="game-info">
//           <div>{/* status */}</div>
//           <ol>{/* TODO */}</ol>
//         </div>
//       </div>
//     );
//   }
// }

// // ========================================

// ReactDOM.render(
//   <Game />,
//   document.getElementById('submit_table')
// );
